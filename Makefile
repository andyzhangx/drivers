# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

REGISTRY_NAME=quay.io/k8scsi
IMAGE_NAME=azurefileplugin
IMAGE_VERSION=canary
IMAGE_TAG=$(REGISTRY_NAME)/$(IMAGE_NAME):$(IMAGE_VERSION)
REV=$(shell git describe --long --tags --dirty)

.PHONY: all azurefile clean azurefile-container

all: azurefile

test:
	go test github.com/kubernetes-csi/drivers/pkg/... -cover
	go vet github.com/kubernetes-csi/drivers/pkg/...
azurefile:
	if [ ! -d ./vendor ]; then dep ensure -vendor-only; fi
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-X github.com/kubernetes-csi/drivers/pkg/azurefile.vendorVersion=$(REV) -extldflags "-static"' -o _output/azurefileplugin ./app/azurefileplugin
azurefile-container: azurefile
	docker build -t $(IMAGE_TAG) -f ./app/azurefileplugin/Dockerfile .
push: azurefile-container
	docker push $(IMAGE_TAG)
clean:
	go clean -r -x
	-rm -rf _output
