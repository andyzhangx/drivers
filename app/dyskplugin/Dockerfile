FROM ubuntu:16.04
LABEL maintainers="andyzhangx"
LABEL description="dysk CSI Plugin"

RUN apt-get update
RUN apt-get install -y ca-certificates

COPY _output/dyskplugin /dyskplugin
ENTRYPOINT ["/dyskplugin"]
