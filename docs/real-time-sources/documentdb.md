---
description: >-
  DocumentDB is Amazon's fork of MongoDB which have some minor behavioral
  differences. In this guide, we will go over how to collect the necessary
  information to replicate from DocumentDB.
---

# DocumentDB

## Introduction

We will be running Debezium to fetch CDC logs from DocumentDB by using [Change Streams](https://www.mongodb.com/docs/manual/changeStreams/) which is a more performant and reliable approach than tailing the `oplog`.&#x20;

{% hint style="info" %}
To run Artie with DocumentDB, you **must** have SSH tunnels enabled as DocumentDB only allows access within your VPC.  See [enabling-ssh-tunneling.md](../tutorials/enabling-ssh-tunneling.md "mention") for instructions!
{% endhint %}

## Finding your DocumentDB settings

This is necessary so that we are able to run a Debezium deployment to subscribe to Change Events within your MongoDB cluster. To see additional configuration properties, please click [here](https://debezium.io/documentation/reference/2.0/connectors/mongodb.html#mongodb-connector-properties) to see Debezium's documentation.

| Name             | Description                                                                     |
| ---------------- | ------------------------------------------------------------------------------- |
| Cluster endpoint | Click on your DocumentDB cluster and you will find this under `Configuration`.  |
| Port             | The default port is `27017`, change it if you are running it on another port.   |
| Username         | Username for authentication into your database.                                 |
| Password         | Password for authentication into your database.                                 |

<figure><img src="../.gitbook/assets/image (13).png" alt="" width="371"><figcaption></figcaption></figure>

### Supported types

_Types are sourced from the_ [_MongoDB extended JSON specification_](https://github.com/mongodb/specifications/blob/master/source/extended-json.rst#canonical-extended-json-example)_._

* Array
* Binary
* Array
* Boolean
* ObjectID
* Int32
* Int64
* Double
* Decimal128
* Code
* CodeWScope
* RegEx
* Datetime
* Timestamp
* String

