---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your MongoDB server to start providing CDC logs.
---

# MongoDB

## Introduction

We will be running Debezium to fetch CDC logs from MongoDB by using [Change Streams](https://www.mongodb.com/docs/manual/changeStreams/) which is a more performant and reliable approach than tailing the `oplog`.&#x20;

{% hint style="info" %}
MongoDB server **must** be in a replica set. If your deployment only has a `standalone` server, you can create a replica set with one member.
{% endhint %}

## Finding your MongoDB settings

This is necessary so that we are able to run a Debezium deployment to subscribe to Change Events within your MongoDB cluster. To see additional configuration properties, please click [here](https://debezium.io/documentation/reference/2.0/connectors/mongodb.html#mongodb-connector-properties) to see Debezium's documentation.

| Name                      | Description                                                                                                                                                                                                                                                                                                                                 | Default value |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| MongoDB Hosts             | <p>Comma-separated list of hostname and port pairs.<br><br>Example:<br><code>rs0/localhost:27017</code></p>                                                                                                                                                                                                                                 | No default    |
| MongoDB Connection String | <p>if you don't have the list of hostnames, you can also pass in a MongoDB connection string. Click <a href="https://www.mongodb.com/docs/manual/reference/connection-string/">here</a> to see how to retrieve this.<br><br>We only need either MongoDB Hosts <strong>OR</strong> MongoDB connection string. <strong>Not both</strong>.</p> | No default.   |
| MongoDB User              | Username for authentication into your database.                                                                                                                                                                                                                                                                                             | No default    |
| MongoDB Password          | Password for authentication into your database.                                                                                                                                                                                                                                                                                             | No default    |
| MongoDB Auth Database     | MongoDB [authSource](https://www.mongodb.com/docs/manual/reference/connection-string/#mongodb-urioption-urioption.authSource) (which database should we authenticate against)                                                                                                                                                               | admin         |

## Things to note if you are running your own Debezium

{% hint style="info" %}
These considerations are automatically handled for you if Artie Transfer is running a Debezium connector for you.&#x20;

_We are also actively working on reducing the amount of considerations required to support every possible configuration._&#x20;
{% endhint %}

* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
