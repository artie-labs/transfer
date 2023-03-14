---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your MySQL to start providing CDC logs.
---

# WIP - MySQL

## Finding your MySQL settings

This is necessary so that we are able to run a Debezium deployment to read from your MySQL server's binlogs and publish them onto Kafka. To see additional configuration properties, please click [here](https://debezium.io/documentation/reference/2.0/connectors/mysql.html#mysql-connector-properties) to see Debezium's documentation.

We will need the following:

| Name               | Description                                                    | Default value |
| ------------------ | -------------------------------------------------------------- | ------------- |
| Database Host Name | IP address or hostname of your database server.                | No default    |
| Database Port      | Port for where your server is running.                         | `3306`        |
| Database Username  | Username for authentication into your database.                | No default    |
| Database Password  | Password for authentication into your database.                | No default    |
| Database Name      | The name of the database that you want to capture changes for. | No default.   |

### Supported types

* BOOLEAN / BOOL
* BIT(1)
* TINYINT
* SMALLINT\[(M)]
* MEDIUMINT\[(M)]
* INT, INTEGER\[(M)]
* BIGINT\[(M)]
* REAL\[(M, D)]
* FLOAT\[(P)]
* FLOAT(M, D)
* NUMERIC\[(M\[,D])]
* DECIMAL\[M\[,D])]
* DOUBLE\[(M, D)]
* CHAR(M)
* VARCHAR(M)
* TINYTEXT
* TEXT
* MEDIUMTEXT
* LONGTEXT
* JSON
* ENUM
* SET
* YEAR\[(2|4)]
* TIMESTAMP\[(M)]
* DATE
* TIME\[(M)]
* DATETIME, DATETIME(M)

{% hint style="info" %}
If you plan to your own Debezium connector to capture these logs, you can pass in additional parameters, write them to a file and use it to create the Debezium connector. Click [here to see an example](https://github.com/artie-labs/transfer/blob/master/examples/postgres/register-postgres-connector.json).
{% endhint %}

## Things to note if you are running your own Debezium

{% hint style="info" %}
These considerations are automatically handled for you if Artie Transfer is running a Debezium connector for you.&#x20;

_We are also actively working on reducing the amount of considerations required to support every possible configuration._&#x20;
{% endhint %}

* `decimal.handling.mode` works for `double` or `string`.\
  The default value is `precise` which will cast the value in `java.math.BigDecimal` and Transfer does not know how to decode that yet. For further information on how to set this to be `string` or `double`, please [click here](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-debezium.html#connector-details)
* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
* Transfer only supports `time.precision.mode=adaptive` which is the default value
