# Setting up Pub/Sub

## Overview

In this tutorial, we will learn how to run Debezium Server with Pub/Sub sink and Artie Transfer locally using Docker.

<figure><img src="../.gitbook/assets/image (5).png" alt=""><figcaption></figcaption></figure>

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

## Creating a service account

## Download the service account credentials

Once your service account has been created, head to the GCP console and create a key for the service account. Save the key as we will be referencing it in the later steps.

<figure><img src="../.gitbook/assets/image (2).png" alt=""><figcaption></figcaption></figure>

## Create the Pub/Sub topic and subscriptions

## Running Debezium

## Running Transfer

## Check Results



## Closing remarks

We hope you found this tutorial helpful.&#x20;

* The code for this tutorial can be found HERE.&#x20;
* To understand how Artie Transfer works with Google Pub/Sub under the hood, please click on [this link.](https://docs.google.com/document/d/1scNkmFS8FEG-GqSKe9bcRwxrqk8us7waPJr5eNbtoCk/edit?usp=sharing)
* If you run into any other issues, please file a bug report on our GitHub page or get in touch at `hi@artie.so`.



