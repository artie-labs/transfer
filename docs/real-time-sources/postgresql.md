---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your PostgreSQL to start providing CDC logs.
---

# PostgreSQL

## Finding your PostgreSQL settings

This is necessary so that we are able to run a Debezium deployment to read from your PostgreSQL server's replication slot and publish them onto Kafka. To see additional configuration properties, please click [here](https://debezium.io/documentation/reference/2.0/connectors/postgresql.html#postgresql-connector-properties) to see Debezium's documentation.

We will need the following:

| Name               | Description                                                    | Default value |
| ------------------ | -------------------------------------------------------------- | ------------- |
| Database Host Name | IP address or hostname of your database server.                | No default    |
| Database Port      | Port for where your server is running.                         | `5432`        |
| Database Username  | Username for authentication into your database.                | No default    |
| Database Password  | Password for authentication into your database.                | No default    |
| Database Name      | The name of the database that you want to capture changes for. | No default.   |

## Running Debezium and Transfer yourself

### Sample Debezium Configuration

{% hint style="info" %}
If you plan to run your own Debezium connector to capture these logs, you can pass in additional parameters, write them to a file and use it to create the Debezium connector. Click [here to see an example](https://github.com/artie-labs/transfer/blob/master/examples/postgres/register-postgres-connector.json).
{% endhint %}

## Creating your replication slot

By running Debezium we support 2 different plugins, which are `pgoutput` and `decoderbufs`.

* `pgoutput` is a default plug-in for PostgreSQL 10+. This is also what PostgreSQL uses for its own logical replication.
* `decoderbufs` is based on Protobuf and is maintained by the Debezium community. To use this, you would need to configure additional libraries.

{% hint style="info" %}
There is also a third option, `wal2json`, which is a deprecated plugin that has been removed from Debezium 2.0.&#x20;

If you have a hard requirement on using this, please get in touch with us.
{% endhint %}

```sql
-- Creating a replication slot, see here for more details:
-- https://www.postgresql.org/docs/9.4/catalog-pg-replication-slots.html
SELECT * FROM pg_create_logical_replication_slot('debezium', 'pgoutput');

-- List all the available replication slots
SELECT * FROM pg_replication_slots;
```

#### Self-hosted notes:

{% hint style="info" %}
These considerations are automatically handled for you if Artie Transfer is running a Debezium connector for you.&#x20;

_We are also actively working on reducing the amount of considerations required to support every possible configuration._&#x20;
{% endhint %}

* `decimal.handling.mode` only works for `double` or `string`.\
  The default value is `precise` which will cast the value in `java.math.BigDecimal` and Transfer does not know how to decode that yet. For further information on how to set this to be `string` or `double`, please [click here](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-debezium.html#connector-details)
* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
* Transfer only supports `time.precision.mode=adaptive` which is the default value
