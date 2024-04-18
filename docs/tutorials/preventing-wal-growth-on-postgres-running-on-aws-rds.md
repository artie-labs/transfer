---
description: >-
  In this document, we will discuss how to prevent WAL growth for a Postgres
  database running on AWS RDS.
---

# Preventing WAL growth on Postgres running on AWS RDS

Last updated: 04/17/2024

## What is WAL?

WAL stands for Write-Ahead Logging, which is a method for Postgres to handle change data capture (CDC). Database record changes will be logged and stored within WAL to ensure data integrity.&#x20;

The WAL also makes it accessible to downstream applications to subscribe to a replication slot and consume database CDC changes reliably.

WAL growth is a problem and can result in replication slot overflow. Replication slot overflow happens when the WAL accumulates and grows, consumes all your database’s storage and causes your database to go down.&#x20;

### Why is WAL growth an issue on AWS RDS?

As Gunnar Morling covered extensively in his blog post [here](https://www.morling.dev/blog/insatiable-postgres-replication-slot/), AWS RDS periodically writes a heartbeat to a table within `rdsadmin` every 5 minutes. AWS RDS has a default setting of 64MB for each WAL segment size, so each heartbeat takes up 64MB of memory. The purpose for AWS writing heartbeats is for various reasons, including to monitor the health of their databases.&#x20;

An important note is that the heartbeats are written to a table within an internal database that is not observed by most CDC applications. For an active database, heartbeats don’t cause an issue because the WAL is almost constantly being drained as new CDC logs are processed. However, if you have a test database that has low traffic or an idle database, you will see your WAL accumulate by 64MB every 5 minutes, or 18.4GB per day! If left unchecked, this can cause replication slot overflow.&#x20;

### Enable Heartbeats for idle/low traffic databases

You only need to enable this feature if your database is low traffic or idle for long periods of time, which are primarily test databases. This feature is not necessary for active databases because the WAL growth will reset as soon as there are data changes from the table(s) you are observing.

### Use the Heartbeat feature to prevent WAL growth

Debezium has a [Heartbeat feature](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-heartbeat-action-query) that allows us to periodically ping the database to create a CDC event and prevent WAL  growth for idle/low traffic databases.&#x20;

1. To turn this on with Artie, first create a heartbeat\_table within Postgres:

```sql
CREATE TABLE test_heartbeat_table (id text PRIMARY KEY, ts timestamp);
-- Then insert one row into this table.
-- Artie's periodic pings will be this:
-- UPDATE test_heartbeat_table set ts = now() where id = '1';
-- Such that we never end up adding additional rows.
INSERT INTO test_heartbeat_table (id, ts) VALUES (1, NOW());
```

2. Check `Enable Heartbeats` under Deployment Advanced Settings

<figure><img src="../.gitbook/assets/image (35).png" alt=""><figcaption></figcaption></figure>

## If you do have heartbeats enabled

* Does the heartbeats table (`test_heartbeat_table`) actually exist?
* Is the heartbeats table included in your Postgres PUBLICATIONS?
* Does the service account have permissions to write to the table?

## Monitor any long-running transactions

Are there any long-running queries that may prevent your replication slot from being advanced? Check if there are any long running queries by running this:

```sql
SELECT
  pid,
  now() - pg_stat_activity.query_start AS duration,
  query,
  state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '1 minute';
```

## Additional preventative measures

In addition to enabling heartbeats, it is best practice to set up the following:

* Monitoring your Amazon RDS instance for `free_storage_space`.
* Implement `statement_timeout` so long running transactions do not block replication slots from advancing. \[[AWS guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Parameters.html)]
* Enable storage autoscaling. The guide to enable this can be [found here](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER\_PIOPS.StorageTypes.html#USER\_PIOPS.Autoscaling).
* Configure [max\_slot\_wal\_keep\_size](https://www.postgresql.org/docs/current/runtime-config-replication.html) to the desired size
  * The default value is -1
  * Each file size is 64mb
  * If you want to set this to be 1 GB, set `max_slot_wal_keep_size` to be 16

### Advanced commands

<pre class="language-sql"><code class="lang-sql">-- See all replication slots
SELECT * FROM pg_replication_slots;

-- Drop replication slot
select pg_drop_replication_slot(REPLICATION_SLOT_NAME);

-- See the size of replication slot
<strong>SELECT
</strong>  slot_name,
  pg_size_pretty(
    pg_wal_lsn_diff(
      pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
  active,
  restart_lsn FROM pg_replication_slots;
</code></pre>
