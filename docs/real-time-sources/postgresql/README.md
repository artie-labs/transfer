---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your PostgreSQL to start providing CDC logs.
---

# PostgreSQL

## Finding your PostgreSQL settings

This is necessary so that we are able to run a Debezium deployment to read from your PostgreSQL server's replication slot and publish them onto Kafka. To see additional configuration properties, please click [here](https://debezium.io/documentation/reference/2.0/connectors/postgresql.html#postgresql-connector-properties) to see Debezium's documentation.

We will need the following:

| Name                        | Description                                                                                                                                       | Default value |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| Host Name                   | IP address or hostname of your database server.                                                                                                   | No default    |
| Port                        | Port for where your server is running.                                                                                                            | `5432`        |
| <p>Username<br>Password</p> | <p>Username and Password for authentication into your database.<br><br>See below if you'd like to create a service account specific to Artie.</p> | No default    |
| Database                    | The name of the database that you want to capture changes for.                                                                                    | No default.   |

### Creating a new user

{% hint style="info" %}
**Using Amazon RDS?** RDS has its own internal permissioning model. Run this command instead of `ALTER USER REPLICATION`!

`GRANT rds_replication to username;`
{% endhint %}

### Granting access

```sql
CREATE USER artie_transfer WITH PASSWORD 'password';

-- (optional) If the schema is not public, you will need this additional line
GRANT USAGE ON SCHEMA schema_name TO artie_transfer;

-- Grant read-only access to future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA schema_name GRANT SELECT ON TABLES TO artie_transfer;
-- Grant access to existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA schema_name TO artie_transfer;

-- The replication role does not have enough permissions to create publications. 
-- So you will need to create this as well.
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Add the replication role to your user (not needed for Amazon RDS)
ALTER USER artie_transfer REPLICATION;
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
* `HSTORE`
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
* `PostGIS data type` (following [GeoJSON feature spec](https://datatracker.ietf.org/doc/html/rfc7946))
  * `Latitude`
  * `Longitude`
  * `Point`
  * `Geometry`
  * `Geography`
  * More coming soon!

## Additional features

### Heartbeats

(following GeoJSON feature spec)

Once you selected `Enable heartbeats` under Advanced Settings, you will then need to run the following command:

```sql
CREATE TABLE test_heartbeat_table (id text PRIMARY KEY, ts timestamp);
-- Grant access to the heartbeat table
GRANT UPDATE ON TABLE test_heartbeat_table TO artie_transfer;
-- Then insert one row into this table.
-- Artie's periodic pings will be this:
-- UPDATE test_heartbeat_table set ts = now() where id = '1';
-- Such that we never end up adding additional rows.
INSERT INTO test_heartbeat_table (id, ts) VALUES (1, NOW());
```

### Customize PostgreSQL publications

By default, Artie will create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) that includes all table changes. You can override this behavior by selecting `Filtered` under the Deployment advanced settings.

**If you change this to be filtered**, this means that we will update the publications whenever tables get added or removed. Additionally, the service account must be the owner of the tables as this is a [PostgreSQL requirement](https://www.postgresql.org/docs/current/sql-alterpublication.html).&#x20;

<figure><img src="../../.gitbook/assets/image (1) (1) (1) (1).png" alt="" width="563"><figcaption><p>Changing the behavior of Postgres publications</p></figcaption></figure>

### PostgreSQL Watcher

To set up your PostgreSQL database for CDC-based replication, you will need to enable replication slots. When this is done incorrectly, it could potentially cause a replication slot overflow and bring your production database down.

PostgreSQL Watcher provides additional guardrails around your database replication, and will do the following:

* **Regularly check and monitor** your replication slot size in 15-minute intervals and notify if the slot exceeds a certain threshold.
* **Heartbeats verification.** For folks that are leveraging [Heartbeats](https://docs.artie.so/tutorials/preventing-wal-growth-on-postgres-running-on-aws-rds), PostgreSQL Watcher will also check to make sure table permissions are updated and our service account has access to run Heartbeats. Watcher will notify you if the verification fails.
* **Terminate any idle queries** that are lingering for more than a day. You can avoid having this problem by setting `idle_in_transaction_session_timeout`.&#x20;

PostgreSQL Watcher is available to all Artie Cloud customers using PostgreSQL as a data source.&#x20;

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
