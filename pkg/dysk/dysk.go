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
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"

	"github.com/kubernetes-csi/drivers/pkg/csi-common"
)

type DyskDriver struct {
	driver *csicommon.CSIDriver

	ids *identityServer
	ns  *nodeServer
	cs  *controllerServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

var (
	dyskDriver *DyskDriver
	vendorVersion  = "0.2.0"
)

func GetDyskDriver() *DyskDriver {
	return &DyskDriver{}
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

func (d *DyskDriver) Run(driverName, nodeID, endpoint string) {
	glog.Infof("Driver: %v ", driverName)

	// Initialize default library driver
	d.driver = csicommon.NewCSIDriver(driverName, vendorVersion, nodeID)
	if d.driver == nil {
		glog.Fatalln("Failed to initialize CSI Driver.")
	}
	d.driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME})
	d.driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	// Create GRPC servers
	d.ids = NewIdentityServer(d.driver)
	d.ns = NewNodeServer(d.driver)
	d.cs = NewControllerServer(d.driver)

	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(endpoint, d.ids, d.cs, d.ns)
	s.Wait()
}
