FROM alpine
LABEL maintainers="Kubernetes Authors"
LABEL description="AzureFile Driver"

COPY ./_output/azurefileplugin /azurefileplugin
ENTRYPOINT ["/azurefileplugin"]
