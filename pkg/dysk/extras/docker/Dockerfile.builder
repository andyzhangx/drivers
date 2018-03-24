FROM golang:alpine
LABEL maintainers="andyzhangx"
LABEL description="dysk CSI Plugin"

RUN apk add --no-cache git make wget
RUN wget https://github.com/golang/dep/releases/download/v0.3.2/dep-linux-amd64 && \
	chmod +x dep-linux-amd64 && \
	mv dep-linux-amd64 /usr/bin/dep
RUN go get -d github.com/kubernetes-csi/drivers/app/dyskplugin
RUN cd /go/src/github.com/kubernetes-csi/drivers && \
	dep ensure && \
	make dysk && \
	cp _output/dyskplugin /dyskplugin
