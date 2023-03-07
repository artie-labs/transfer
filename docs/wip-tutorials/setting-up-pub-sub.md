# Setting up Pub/Sub

## Overview

In this tutorial, we will learn how to run Debezium Server with Pub/Sub sink and Artie Transfer.&#x20;

## Pre-requisites

* Terraform
* Docker
* gcloud CLI
* GCP Project

### Set-up

**gcloud CLI**

Please visit [this link](https://cloud.google.com/sdk/docs/install) to download the CLI. Once you have done so, run this command:

```bash
gcloud auth application-default login
```

**Pub/Sub API**

To use Pub/Sub in your GCP project, you will also need to enable it. Visit [this link](https://console.cloud.google.com/marketplace/product/google/pubsub.googleapis.com) to enable it.

<figure><img src="../.gitbook/assets/image (1).png" alt=""><figcaption></figcaption></figure>

