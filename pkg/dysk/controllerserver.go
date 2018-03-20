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

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"github.com/khenidak/dysk/pkg/client"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/pborman/uuid"

	"golang.org/x/net/context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	//"k8s.io/kubernetes/pkg/volume/util"
)

const (
	autoLeaseFlag  = true
	vhdFlag        = true
	breakLeaseFlag = true
)

var (
	blob      = ""
	container = ""
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Volume Name
	volName := req.GetName()
	if len(volName) == 0 {
		volName = uuid.NewUUID().String()
	}

	// Volume Size - Default is 1 GiB
	volSizeBytes := int64(1 * 1024 * 1024 * 1024)
	if req.GetCapacityRange() != nil {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeGB := uint(volSizeBytes / 1024 / 1024 / 1024)

	secrets := req.GetControllerCreateSecrets()
	storageAccountName, storageAccountKey, err := getStorageAccount(secrets)
	if err != nil {
		return nil, err
	}

	parameters := req.GetParameters()
	containerName, blobName, err := getContainerBlob(parameters)
	if err != nil {
		return nil, err
	}

	container = containerName
	blob = blobName

	dyskClient := client.CreateClient(storageAccountName, storageAccountKey)
	glog.V(4).Infof("begin to create page blob(%s) in container(%s), account:%s, size:%dGB", blob, container, storageAccountName, volSizeGB)

	leaseID, err := dyskClient.CreatePageBlob(volSizeGB, container, blob, vhdFlag, autoLeaseFlag)
	if err != err {
		glog.Errorf("create page blob failed, error:%v", err)
		return nil, err
	}
	glog.V(4).Infof("create page blob successfully, leaseID:%q", leaseID)

	volumeID := storageAccountName + "/" + container + "/" + blob
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id: volumeID,
			Attributes: map[string]string{
				"container": container,
				"blob":      blob,
				"leaseID":   leaseID,
			},
		},
	}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid delete volume req: %v", req)
		return nil, err
	}
	volumeID := req.VolumeId
	glog.V(4).Infof("deleting volume %s", volumeID)

	secrets := req.GetControllerDeleteSecrets()
	storageAccountName, storageAccountKey, err := getStorageAccount(secrets)
	if err != nil {
		return nil, err
	}

	dyskClient := client.CreateClient(storageAccountName, storageAccountKey)
	if err := dyskClient.DeletePageBlob(container, blob, "", breakLeaseFlag); nil != err {
		return nil, err
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Supported: true, Message: ""}, nil
}

func getStorageAccount(secrets map[string]string) (string, string, error) {
	if secrets == nil {
		return "", "", fmt.Errorf("unexpected: getStorageAccount ControllerCreateSecrets is nil")
	}

	storageAccountName, ok := secrets["accountname"]
	if !ok {
		return "", "", fmt.Errorf("could not find accountname field in CreateVolume ControllerCreateSecrets")
	}
	storageAccountKey, ok := secrets["accountkey"]
	if !ok {
		return "", "", fmt.Errorf("could not find accountkey field in CreateVolume ControllerCreateSecrets")
	}

	return storageAccountName, storageAccountKey, nil
}

func getContainerBlob(parameters map[string]string) (string, string, error) {
	if parameters == nil {
		return "", "", fmt.Errorf("unexpected: getContainerBlob Parameters is nil")
	}

	container, ok := parameters["container"]
	if !ok {
		return "", "", fmt.Errorf("could not find container field in CreateVolume Parameters")
	}
	blob, ok := parameters["blob"]
	if !ok {
		return "", "", fmt.Errorf("could not find blob field in CreateVolume Parameters")
	}

	return container, blob, nil
}
