---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your MySQL to start providing CDC logs.
---

# MySQL

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

* `BOOLEAN` / `BOOL`
* `BIT(1)`
* `TINYINT`
* `SMALLINT[(M)]`
* `MEDIUMINT[(M)]`
* `INT, INTEGER[(M)]`
* `BIGINT[(M)]`
* `REAL[(M, D)]`
* `FLOAT[(P)]`
* `FLOAT(M, D)`
* `NUMERIC[(M[,D])]`
* `DECIMAL[M[,D])]`
* `DOUBLE[(M, D)]`
* `CHAR(M)`
* `VARCHAR(M)`
* `TINYTEXT`
* `TEXT`
* `MEDIUMTEXT`
* `LONGTEXT`
* `JSON`
* `ENUM`
* `SET`
* `YEAR[(2|4)]`
* `TIMESTAMP[(M)]`
* `DATE`
* `TIME[(M)]`
* `DATETIME, DATETIME(M)`

## Running it yourself

{% hint style="info" %}
These considerations are automatically handled for you if Artie Transfer is running a Debezium connector for you.&#x20;

_We are also actively working on reducing the amount of considerations required to support every possible configuration._&#x20;
{% endhint %}

* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
* [Example Debezium connector settings](https://github.com/artie-labs/transfer/blob/master/examples/postgres/register-postgres-connector.json)
