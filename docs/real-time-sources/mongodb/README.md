---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your MongoDB server to start providing CDC logs.
---

# MongoDB

## Introduction

We will be running Debezium to fetch CDC logs from MongoDB by using [Change Streams](https://www.mongodb.com/docs/manual/changeStreams/) which is a more performant and reliable approach than tailing the `oplog`.&#x20;

{% hint style="info" %}
MongoDB server **must** be in a replica set. If your deployment only has a `standalone` server, you can create a replica set with one member.&#x20;

\
Need help? Check out this [guide](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).
{% endhint %}

## Finding your MongoDB settings

This is necessary so that we are able to run a Debezium deployment to subscribe to Change Events within your MongoDB cluster. To see additional configuration properties, please click [here](https://debezium.io/documentation/reference/2.0/connectors/mongodb.html#mongodb-connector-properties) to see Debezium's documentation.

| Name                  | Description                                                                                                                                                                   | Default value |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| Connection string     | Click [here](https://www.mongodb.com/docs/manual/reference/connection-string/) to see how to retrieve this.                                                                   | No default.   |
| Username              | Username for authentication into your database.                                                                                                                               | No default    |
| Password              | Password for authentication into your database.                                                                                                                               | No default    |
| Authentication source | MongoDB [authSource](https://www.mongodb.com/docs/manual/reference/connection-string/#mongodb-urioption-urioption.authSource) (which database should we authenticate against) | admin         |

### Creating a new user

```mongodb
use admin;

# Creating a role to allow listing DBs and finding the changeStream
db.runCommand({
    createRole: "listDatabases",
    privileges: [
        { resource: { cluster : true }, actions: ["listDatabases"]}
    ],
    roles: []
});

db.runCommand({
    createRole: "readChangeStream",
    privileges: [
        { resource: { db: "", collection: ""}, actions: [ "find", "changeStream" ] },
    ],
    roles: []
});

# Creating a service account, assign permissions
db.createUser({
    user: 'artie',
    pwd: 'artie',
    roles: [
        { role: "read", db: <COLLECTION_NAME> },
        { role: "read", db: "local" },
        { role: "listDatabases", db: "admin" },
        { role: "readChangeStream", db: "admin" },
        { role: "read", db: "config" },
        { role: "read", db: "admin" }
    ]
});
```

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

## Running it yourself

#### Self-hosted notes:

{% hint style="info" %}
These considerations are automatically handled for you if Artie Transfer is running a Debezium connector for you.&#x20;

_We are also actively working on reducing the amount of considerations required to support every possible configuration._&#x20;
{% endhint %}

* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
* [Example Debezium connector settings](https://github.com/artie-labs/transfer/blob/master/examples/mongodb/register-mongodb-connector.json)
