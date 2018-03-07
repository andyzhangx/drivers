# CSI dysk driver

## Usage:

### Build dyskplugin
```
$ make dysk
```

### Prerequisite
set azure storage account name and key
```
export X_CSI_SECRETS="accountname=xiazhang,accountkey=RJwctkX+HLU8yIh+LpOMubegRFDYKdvSv4sMbM/SOk8wugDVQ+DUEVxtpBskvgCDWUo3VvRemoved=="
```

### Start dysk driver
```
$ sudo ./_output/dyskplugin --endpoint tcp://127.0.0.1:10000 --nodeid CSINode -v=5
```

### Test using csc
Get ```csc``` tool from https://github.com/thecodeteam/gocsi/tree/master/csc

#### Get plugin info
```
$ csc identity plugin-info --endpoint tcp://127.0.0.1:10000
"csi-dysk"  "0.1.0"
```

#### Get supported versions
```
$ csc identity supported-versions --endpoint tcp://127.0.0.1:10000
0.1.0
```

#### Create a volume
```
$ csc controller new --endpoint tcp://127.0.0.1:10000 --cap 1,block CSIVolumeName  --req-bytes 2147483648 --params container=public,blob=test2.vhd
CSIVolumeID
```

#### Delete a volume
```
$ csc controller del --endpoint tcp://127.0.0.1:10000 CSIVolumeID
CSIVolumeID
```

#### Validate volume capabilities
```
$ csc controller validate-volume-capabilities --endpoint tcp://127.0.0.1:10000 --cap 1,block CSIVolumeID
CSIVolumeID  true
```

#### NodePublish a volume
```
$ csc node publish --endpoint tcp://127.0.0.1:10000 --cap 1,block --target-path /mnt/dysk CSIVolumeID
CSIVolumeID
```

#### NodeUnpublish a volume
```
$ csc node unpublish --endpoint tcp://127.0.0.1:10000 --target-path /mnt/dysk CSIVolumeID
CSIVolumeID
```

#### Get NodeID
```
$ csc node get-id --endpoint tcp://127.0.0.1:10000
CSINode
```
