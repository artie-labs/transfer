---
description: In this section, we will go over how to install and run Artie Transfer.
---

# Overview

## Install

There are three ways to install Transfer:

1. Download the [image from Dockerhub](https://hub.docker.com/r/artielabs/transfer/tags)
2. Git clone the repo and run `go build`
3. &#x20;`go get github.com/artie-labs/transfer` inside of your Go project

Soon, we will use a release manager CI and also have it published alongside our Github project.

## Available runtime flags

When running Transfer (as a binary or as a Go application), you can pass in the following flags:

* `-v` or `--verbose` to have additional logging emitted from Transfer
* `-c` or `--config` (see below) to specify the path of the configuration file.

## What is a configuration file?

{% hint style="info" %}
Transfer expects the configuration file to be in [YAML](https://yaml.org/) format.
{% endhint %}

The configuration file is used to inform each Transfer deployment of the workload(s) required. Inside of the configuration file, we can specify things like:

* Which Kafka server should Transfer connect to?
* What is the format of the partition key?
* What does the message format look like?
* Which destination is it going to?

## How do I specify the configuration file?

Whether you are running Transfer locally, using a Docker image or within your Kubernetes cluster. You can specify the configuration file by using the `--config` option.

**Running this as a standalone binary (Docker / Kubernetes)**

```bash
/transfer --config config.yaml
```

**Running this as a Go project**

```go
go run main.go --config config.yaml
```

## Next

To see all the available settings, please click on the link below to continue.

{% content-ref url="options.md" %}
[options.md](options.md)
{% endcontent-ref %}
