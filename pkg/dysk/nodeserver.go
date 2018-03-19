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

	options := []string{""}
	if readOnly {
		options = append(options, "ro")
	}

	secrets := req.GetNodePublishSecrets()
	storageAccountName, storageAccountKey, err := getStorageAccount(secrets)
	if err != nil {
		return nil, err
	}

	dyskClient := client.CreateClient(storageAccountName, storageAccountKey)

	glog.V(4).Infof("begin to mount page blob(%s) in container(%s), account:%s", blob, container, storageAccountName)

	d := client.Dysk{}
	d.Type = client.ReadWrite
	if readOnlyFlag {
		d.Type = client.ReadOnly
	}
	d.AccountName = storageAccountName
	d.AccountKey = storageAccountKey
	device = getRandomDyskName()
	d.Name = device
	//hardcoded here, need to get SizeGB somewhere
	d.SizeGB = 2
	d.Path = "/" + container + "/" + blob
	// need to get LeaseId somewhere
	d.LeaseId = ""
	d.Vhd = vhdFlag

	err = dyskClient.Mount(&d, autoLeaseFlag, breakLeaseFlag)
	if err != nil {
		return nil, fmt.Errorf("mount page blob failed, error: %v", err)
	}

	mountInterface := mount.New("" /* default mount path */)
	mounter := mount.SafeFormatAndMount{
		Interface: mountInterface,
		Exec:      mount.NewOsExec(),
	}

	err = os.MkdirAll(targetPath, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return nil, fmt.Errorf("mkdir %s failed, error: %v", targetPath, err)
		}
	}

	mounter.FormatAndMount("/dev/" + device, targetPath, fsType, options)
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

	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	dyskClient := client.CreateClient("", "")
	if err = dyskClient.Unmount(device, breakLeaseFlag); err != nil {
		return nil, err
	}

	// clean TargetPath next
	/*
		// Unmounting the image
		err = mount.New("").Unmount(req.GetTargetPath())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	*/
	glog.V(4).Infof("dysk: volume %s/%s has been unmounted.", targetPath, volumeID)

	return &csi.NodeUnpublishVolumeResponse{}, nil
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
	var of = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	out := make([]rune, 8)
	for i := range out {
		out[i] = of[rand.Intn(len(of))]
	}
	return "dysk" + string(out)
}
