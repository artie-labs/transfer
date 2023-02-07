---
description: >-
  In this section, we will explore the various options available within the
  Transfer configuration and provide examples for how to get started.
---

# Configurations

### What is a configuration file?

The configuration file is used to inform each Transfer deployment of the workload(s) required. Inside of the configuration file, we can specify things like:

* Which Kafka server should Transfer connect to?
* What is the format of the partition key?
* What does the message format look like?
* Which destination is it going to?

### How do I specify the configuration file?

Whether you are running Transfer locally, using a Docker image or within your Kubernetes cluster...you can specify the configuration file by using the `--config` option.

**Running this as a standalone binary (Docker / Kubernetes)**

```bash
/transfer --config config.yaml
```

**Running this as a Go project**

```go
go run main.go --config config.yaml
```

### Next

To see the available settings within a configuration file, please click on the link below to continue.

{% content-ref url="configurations/options.md" %}
[options.md](configurations/options.md)
{% endcontent-ref %}
