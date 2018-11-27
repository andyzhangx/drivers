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
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/kubernetes/pkg/cloudprovider/providers/azure"
	"k8s.io/kubernetes/pkg/volume/util"

	utilexec "k8s.io/utils/exec"

	//"github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2018-07-01/storage"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/pborman/uuid"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	deviceID      = "deviceID"
	provisionRoot = "/tmp/"
	snapshotRoot  = "/tmp/"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	cloud *azure.Cloud
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.Errorf("invalid create volume req: %v", req)
		return nil, err
	}

	// Validate arguments
	volumeCapabilities := req.GetVolumeCapabilities()
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}
	if volumeCapabilities == nil || len(volumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	requestGiB := int(util.RoundUpSize(volSizeBytes, 1024*1024*1024))

	parameters := req.GetParameters()
	var sku, resourceGroup, location, account string

	// File share name has a length limit of 63, and it cannot contain two consecutive '-'s.
	// todo: get cluster name
	fileShareName := util.GenerateVolumeName("pvc-file", uuid.NewUUID().String(), 63)
	fileShareName = strings.Replace(fileShareName, "--", "-", -1)
	// Apply ProvisionerParameters (case-insensitive). We leave validation of
	// the values to the cloud provider.
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case "skuname":
			sku = v
		case "location":
			location = v
		case "storageaccount":
			account = v
		case "resourcegroup":
			resourceGroup = v
		default:
			return nil, fmt.Errorf("invalid option %q", k)
		}
	}

	// when use azure file premium, account kind should be specified as FileStorage
	/* todo: depends on k8s v1.13.0
	accountKind := string(storage.StorageV2)
	if strings.HasPrefix(strings.ToLower(sku), "premium") {
		accountKind = string(storage.FileStorage)
	}
	*/

	glog.V(2).Infof("begin to create file share(%s) on account(%s) type(%s) rg(%s) location(%s) size(%d)", fileShareName, account, sku, resourceGroup, location, requestGiB)
	account, key, err := cs.cloud.CreateFileShare(fileShareName, account, sku, resourceGroup, location, requestGiB)
	if err != nil {
		glog.Errorf("failed to create volume: %v", err)
		return nil, err
	}
	parameters[ACCOUNT_NAME] = account
	parameters[ACCOUNT_KEY] = key

	volumeID := account + "-" + fileShareName

	if req.GetVolumeContentSource() != nil {
		contentSource := req.GetVolumeContentSource()
		if contentSource.GetSnapshot() != nil {
			snapshotId := contentSource.GetSnapshot().GetId()
			snapshot, ok := azureFileVolumeSnapshots[snapshotId]
			if !ok {
				return nil, status.Errorf(codes.NotFound, "cannot find snapshot %v", snapshotId)
			}
			if snapshot.Status.Type != csi.SnapshotStatus_READY {
				return nil, status.Errorf(codes.Internal, "status of snapshot %v is not ready", snapshotId)
			}
			snapshotPath := snapshot.Path
			path := provisionRoot
			args := []string{"zxvf", snapshotPath, "-C", path}
			executor := utilexec.New()
			out, err := executor.Command("tar", args...).CombinedOutput()
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("failed pre-populate data for volume: %v: %s", err, out))
			}
		}
	}
	glog.V(2).Infof("create file share %s on storage account %s successfully", fileShareName, account)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            volumeID,
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			Attributes:    parameters,
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
	glog.V(2).Infof("deleting azure file %s", volumeID)
	//cs.cloud.DeleteFileShare()

	path := provisionRoot + volumeID
	os.RemoveAll(path)
	delete(azureFileVolumes, volumeID)
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
	if _, ok := azureFileVolumes[req.GetVolumeId()]; !ok {
		return nil, status.Error(codes.NotFound, "Volume does not exist")
	}

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Supported: false, Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Supported: true, Message: ""}, nil
}

// CreateSnapshot uses tar command to create snapshot for azurefile volume. The tar command can quickly create
// archives of entire directories. The host image must have "tar" binaries in /bin, /usr/sbin, or /usr/bin.
func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT); err != nil {
		glog.V(3).Infof("invalid create snapshot req: %v", req)
		return nil, err
	}

	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	// Check arguments
	if len(req.GetSourceVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "SourceVolumeId missing in request")
	}

	// Need to check for already existing snapshot name, and if found check for the
	// requested sourceVolumeId and sourceVolumeId of snapshot that has been created.
	if exSnap, err := getSnapshotByName(req.GetName()); err == nil {
		// Since err is nil, it means the snapshot with the same name already exists need
		// to check if the sourceVolumeId of existing snapshot is the same as in new request.
		if exSnap.VolID == req.GetSourceVolumeId() {
			// same snapshot has been created.
			return &csi.CreateSnapshotResponse{
				Snapshot: &csi.Snapshot{
					Id:             exSnap.Id,
					SourceVolumeId: exSnap.VolID,
					CreatedAt:      exSnap.CreateAt,
					SizeBytes:      exSnap.SizeBytes,
					Status:         exSnap.Status,
				},
			}, nil
		}
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("snapshot with the same name: %s but with different SourceVolumeId already exist", req.GetName()))
	}

	volumeID := req.GetSourceVolumeId()
	azureFileVolume, ok := azureFileVolumes[volumeID]
	if !ok {
		return nil, status.Error(codes.Internal, "volumeID is not exist")
	}

	snapshotID := uuid.NewUUID().String()
	createAt := time.Now().UnixNano()
	volPath := azureFileVolume.VolPath
	file := snapshotRoot + snapshotID + ".tgz"
	args := []string{"czf", file, "-C", volPath, "."}
	executor := utilexec.New()
	out, err := executor.Command("tar", args...).CombinedOutput()
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed create snapshot: %v: %s", err, out))
	}

	glog.V(4).Infof("create volume snapshot %s", file)
	snapshot := azureFileSnapshot{}
	snapshot.Name = req.GetName()
	snapshot.Id = snapshotID
	snapshot.VolID = volumeID
	snapshot.Path = file
	snapshot.CreateAt = createAt
	snapshot.SizeBytes = azureFileVolume.VolSize
	snapshot.Status = &csi.SnapshotStatus{
		Type:    csi.SnapshotStatus_READY,
		Details: fmt.Sprint("Successfully create snapshot"),
	}

	azureFileVolumeSnapshots[snapshotID] = snapshot

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			Id:             snapshot.Id,
			SourceVolumeId: snapshot.VolID,
			CreatedAt:      snapshot.CreateAt,
			SizeBytes:      snapshot.SizeBytes,
			Status:         snapshot.Status,
		},
	}, nil
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	// Check arguments
	if len(req.GetSnapshotId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID missing in request")
	}

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT); err != nil {
		glog.V(3).Infof("invalid delete snapshot req: %v", req)
		return nil, err
	}
	snapshotID := req.GetSnapshotId()
	glog.V(4).Infof("deleting volume %s", snapshotID)
	path := snapshotRoot + snapshotID + ".tgz"
	os.RemoveAll(path)
	delete(azureFileVolumeSnapshots, snapshotID)
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *controllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS); err != nil {
		glog.V(3).Infof("invalid list snapshot req: %v", req)
		return nil, err
	}

	// case 1: SnapshotId is not empty, return snapshots that match the snapshot id.
	if len(req.GetSnapshotId()) != 0 {
		snapshotID := req.SnapshotId
		if snapshot, ok := azureFileVolumeSnapshots[snapshotID]; ok {
			return convertSnapshot(snapshot), nil
		}
	}

	// case 2: SourceVolumeId is not empty, return snapshots that match the source volume id.
	if len(req.GetSourceVolumeId()) != 0 {
		for _, snapshot := range azureFileVolumeSnapshots {
			if snapshot.VolID == req.SourceVolumeId {
				return convertSnapshot(snapshot), nil
			}
		}
	}

	var snapshots []csi.Snapshot
	// case 3: no parameter is set, so we return all the snapshots.
	sortedKeys := make([]string, 0)
	for k := range azureFileVolumeSnapshots {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		snap := azureFileVolumeSnapshots[key]
		snapshot := csi.Snapshot{
			Id:             snap.Id,
			SourceVolumeId: snap.VolID,
			CreatedAt:      snap.CreateAt,
			SizeBytes:      snap.SizeBytes,
			Status:         snap.Status,
		}
		snapshots = append(snapshots, snapshot)
	}

	var (
		ulenSnapshots = int32(len(snapshots))
		maxEntries    = req.MaxEntries
		startingToken int32
	)

	if v := req.StartingToken; v != "" {
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, status.Errorf(
				codes.Aborted,
				"startingToken=%d !< int32=%d",
				startingToken, math.MaxUint32)
		}
		startingToken = int32(i)
	}

	if startingToken > ulenSnapshots {
		return nil, status.Errorf(
			codes.Aborted,
			"startingToken=%d > len(snapshots)=%d",
			startingToken, ulenSnapshots)
	}

	// Discern the number of remaining entries.
	rem := ulenSnapshots - startingToken

	// If maxEntries is 0 or greater than the number of remaining entries then
	// set maxEntries to the number of remaining entries.
	if maxEntries == 0 || maxEntries > rem {
		maxEntries = rem
	}

	var (
		i       int
		j       = startingToken
		entries = make(
			[]*csi.ListSnapshotsResponse_Entry,
			maxEntries)
	)

	for i = 0; i < len(entries); i++ {
		entries[i] = &csi.ListSnapshotsResponse_Entry{
			Snapshot: &snapshots[j],
		}
		j++
	}

	var nextToken string
	if j < ulenSnapshots {
		nextToken = fmt.Sprintf("%d", j)
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func convertSnapshot(snap azureFileSnapshot) *csi.ListSnapshotsResponse {
	entries := []*csi.ListSnapshotsResponse_Entry{
		{
			Snapshot: &csi.Snapshot{
				Id:             snap.Id,
				SourceVolumeId: snap.VolID,
				CreatedAt:      snap.CreateAt,
				SizeBytes:      snap.SizeBytes,
				Status:         snap.Status,
			},
		},
	}

	rsp := &csi.ListSnapshotsResponse{
		Entries: entries,
	}

	return rsp
}
