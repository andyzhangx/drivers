/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dysk

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/khenidak/dysk/pkg/client"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/kubernetes/pkg/util/mount"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Check arguments
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	targetPath := req.GetTargetPath()
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	fsType := req.GetVolumeCapability().GetMount().GetFsType()

	deviceID := ""
	if req.GetPublishInfo() != nil {
		deviceID = req.GetPublishInfo()[deviceID]
	}

	readOnly := req.GetReadonly()
	volumeID := req.GetVolumeId()
	attrib := req.GetVolumeAttributes()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	glog.V(4).Infof("target %v\nfstype %v\ndevice %v\nreadonly %v\nattributes %v\n mountflags %v\n",
		targetPath, fsType, deviceID, readOnly, volumeID, attrib, mountFlags)

	secrets := req.GetNodePublishSecrets()
	storageAccountName, storageAccountKey, err := getStorageAccount(secrets)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	container, blob, err := getContainerBlobByVolumeID(volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	vhdPath := "/" + container + "/" + blob

	dyskClient := client.CreateClient(storageAccountName, storageAccountKey)

	glog.V(4).Infof("begin to mount page blob(%s) in container(%s), account:%s", blob, container, storageAccountName)

	options := []string{}
	d := client.Dysk{}
	if readOnly {
		d.Type = client.ReadOnly
		options = append(options, "ro")
	} else {
		d.Type = client.ReadWrite
	}

	d.AccountName = storageAccountName
	d.AccountKey = storageAccountKey
	device := getRandomDyskName()
	d.Name = device
	d.Path = vhdPath
	d.Vhd = vhdFlag

	if err = dyskClient.Mount(&d, autoLeaseFlag, breakLeaseFlag); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.V(5).Infof("mount page blob complete")

	err = os.MkdirAll(targetPath, os.FileMode(0755))
	if err != nil && !os.IsExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	mounter := mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      mount.NewOsExec(),
	}

	glog.V(4).Infof("begin to format device(%s), targetPath(%s), fsType(%s), options(%q)", "/dev/"+device, targetPath, fsType, options)
	if err = mounter.FormatAndMount("/dev/" + device, targetPath, fsType, options); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.V(5).Infof("format device complete")

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	mountInterface := mount.New("")
	notMnt, err := mountInterface.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	devName, err := getDevName(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = mountInterface.Unmount(targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	glog.V(4).Infof("dysk: volume(%s) targetPath(%s) has been unmounted.", targetPath, volumeID)

	dyskClient := client.CreateClient("", "")
	if err = dyskClient.Unmount(devName, breakLeaseFlag); err != nil {
		return nil, err
	}

	if notMnt {
		err = os.Remove(targetPath)
	}
	return &csi.NodeUnpublishVolumeResponse{}, err
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(req.GetStagingTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func getRandomDyskName() string {
	rand.Seed(time.Now().UTC().UnixNano())
	var of = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	out := make([]rune, 8)
	for i := range out {
		out[i] = of[rand.Intn(len(of))]
	}
	return "dysk" + string(out)
}

func getDevName(targetPath string) (string, error) {
	args := []string{"-o", "SOURCE", "--noheadings", "--first-only", "--target", targetPath}
	output, err := exec.Command("findmnt", args...).CombinedOutput()
	if err != nil {
		return "", err
	}
	out := strings.TrimSuffix(string(output), "\n")
	i := strings.LastIndex(out, "/")
	if i == -1 {
		return "", fmt.Errorf("error parsing findmnt output, expected at least one slash: %q", out)
	}
	return out[i+1:], nil
}
