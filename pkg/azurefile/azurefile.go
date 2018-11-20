/*
Copyright 2017 The Kubernetes Authors.

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

package azurefile

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"

	"github.com/kubernetes-csi/drivers/pkg/csi-common"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100
)

type azureFile struct {
	driver *csicommon.CSIDriver

	ids *identityServer
	ns  *nodeServer
	cs  *controllerServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

type azureFileVolume struct {
	VolName string `json:"volName"`
	VolID   string `json:"volID"`
	VolSize int64  `json:"volSize"`
	VolPath string `json:"volPath"`
}

type azureFileSnapshot struct {
	Name      string              `json:"name"`
	Id        string              `json:"id"`
	VolID     string              `json:"volID"`
	Path      string              `json:"path"`
	CreateAt  int64               `json:"createAt"`
	SizeBytes int64               `json:"sizeBytes"`
	Status    *csi.SnapshotStatus `json:"status"`
}

var azureFileVolumes map[string]azureFileVolume
var azureFileVolumeSnapshots map[string]azureFileSnapshot

var (
	azureFileDriver *azureFile
	vendorVersion   = "dev"
)

func init() {
	azureFileVolumes = map[string]azureFileVolume{}
	azureFileVolumeSnapshots = map[string]azureFileSnapshot{}
}

func GetAzureFileDriver() *azureFile {
	return &azureFile{}
}

func NewIdentityServer(d *csicommon.CSIDriver) *identityServer {
	return &identityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d),
	}
}

func NewControllerServer(d *csicommon.CSIDriver) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
	}
}

func NewNodeServer(d *csicommon.CSIDriver) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
	}
}

func (f *azureFile) Run(driverName, nodeID, endpoint string) {
	glog.Infof("Driver: %v ", driverName)
	glog.Infof("Version: %s", vendorVersion)

	GetCloudProvider()

	// Initialize default library driver
	f.driver = csicommon.NewCSIDriver(driverName, vendorVersion, nodeID)
	if f.driver == nil {
		glog.Fatalln("Failed to initialize azurefile CSI Driver.")
	}
	f.driver.AddControllerServiceCapabilities(
		[]csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		})
	f.driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	// Create GRPC servers
	f.ids = NewIdentityServer(f.driver)
	f.ns = NewNodeServer(f.driver)
	f.cs = NewControllerServer(f.driver)

	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(endpoint, f.ids, f.cs, f.ns)
	s.Wait()
}

func getVolumeByID(volumeID string) (azureFileVolume, error) {
	if azureFileVol, ok := azureFileVolumes[volumeID]; ok {
		return azureFileVol, nil
	}
	return azureFileVolume{}, fmt.Errorf("volume id %s does not exit in the volumes list", volumeID)
}

func getVolumeByName(volName string) (azureFileVolume, error) {
	for _, azureFileVol := range azureFileVolumes {
		if azureFileVol.VolName == volName {
			return azureFileVol, nil
		}
	}
	return azureFileVolume{}, fmt.Errorf("volume name %s does not exit in the volumes list", volName)
}

func getSnapshotByName(name string) (azureFileSnapshot, error) {
	for _, snapshot := range azureFileVolumeSnapshots {
		if snapshot.Name == name {
			return snapshot, nil
		}
	}
	return azureFileSnapshot{}, fmt.Errorf("snapshot name %s does not exit in the snapshots list", name)
}
