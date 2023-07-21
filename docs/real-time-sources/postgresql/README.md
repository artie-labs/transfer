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

### Creating a new user

{% hint style="info" %}
Using Amazon RDS? RDS has its own internal permissioning model. Run this command instead of `ALTER USER REPLICATION`!

`GRANT rds_replication to username;`
{% endhint %}

### Granting access

```sql
CREATE USER username WITH PASSWORD 'password';

-- (optional) If the schema is not public, you will need this additional line
GRANT USAGE ON SCHEMA schema_name TO artie_transfer;

-- Grant read-only access to future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA schema_name GRANT SELECT ON TABLES TO username;
-- Grant access to existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA schema_name TO username;

-- The replication role does not have enough permissions to create publications. 
-- So you will need to create this as well.
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Add the replication role to your user (not needed for Amazon RDS)
ALTER USER username REPLICATION;
```



### Supported types

* `BOOLEAN` / `BOOL`
* `BIT(1)`
* `SMALLINT`, `SMALLSERIAL`
* `INTEGER`, `SERIAL`
* `BIGINT`, `BIGSERIAL`
* `REAL`
* `DOUBLE PRECISION`
* `NUMERIC[(M, [,D])]`
* `DECIMAL[(M, [,D])]`
* `MONEY`
* `CHAR[(M)]`
* `CHARACTER[(M)]`
* `CHARACTER VARYING[(M)]`
* `DATE`
* `TIMESTAMP(M)`
* `TIMESTAMP WITH TIME ZONE`
* `TIME(M)`
* `TIME WITH TIME ZONE`
* `INTERVAL [P]`, we store this in microseconds.
* `JSON`, `JSONB`
* `XML`
* `UUID`
* `CITEXT`
* `INET`
* `CIDR`
* `MACADDR`
* `MACADDR8`
* `INT4RANGE`
* `INT8RANGE`
* `NUMRANGE`
* `TSRANGE`
* `TSTZRANGE`
* `DATERANGE`
* `ENUM`
* `LTREE`

## Running it yourself

#### Self-hosted notes:

{% hint style="info" %}
These considerations are automatically handled for you if Artie Transfer is running a Debezium connector for you.&#x20;

_We are also actively working on reducing the amount of considerations required to support every possible configuration._&#x20;
{% endhint %}

* Debezium will automatically create a replication slot for you.
* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
* [Example Debezium connector settings](https://github.com/artie-labs/transfer/blob/master/examples/postgres/register-postgres-connector.json)
