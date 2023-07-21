---
description: >-
  In this document, we will discuss how to prevent WAL growth for a Postgres
  database running on AWS RDS.
---

# Preventing WAL growth on AWS RDS

## _What is WAL?_

WAL stands for Write-ahead logging, which is a method for Postgres to handle change data capture. Database changes will be recorded and stored within WAL.&#x20;

This makes it accessible to downstream applications to subscribe to a replication slot and consume  database changes reliably.

### So, what's the problem and why is this only an issue on AWS?

As Gunnar Morling covered extensively in his blog post [here](https://www.morling.dev/blog/insatiable-postgres-replication-slot/), RDS will periodically write a heartbeat to a table within `rdsadmin` every 5 minutes. RDS further has a default setting of 64 MB for each WAL segment size.

The problem here is that this is written to a table within an internal database that is not observed by most CDC applications. Meaning, if you have a test database that infrequently gets traffic, you will see your WAL accumulate by \~64 MB every 5 minutes which if left unchecked, can cause a replication slot overflow. This, of course goes away if an observed table made a change and we were able to respond to the WAL changes.

### How do I know if I need to enable Heartbeats?

You only need to enable this feature if your database does not make updates, which are primarily test databases. This is because the WAL growth will reset as soon as there are data changes from the table(s) you are observing.

### What should we do instead?

Within Debezium, there is a [Heartbeat feature](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-heartbeat-action-query) that allows us to periodically ping the database to ensure WAL growth is sustained.&#x20;

1. To turn this on within Artie, first create the table within Postgres:

```sql
CREATE TABLE test_heartbeat_table (id text PRIMARY KEY, ts timestamp);
-- Then insert one row into this table.
-- Artie's periodic pings will be this:
-- UPDATE test_heartbeat_table set ts = now() where id = '1';
-- Such that we never end up adding additional rows.
INSERT INTO test_heartbeat_table (id, ts) VALUES (1, NOW());
```

2. Check `Enable Heartbeats` under Deployment Advanced Settings

<figure><img src="../.gitbook/assets/image (8).png" alt=""><figcaption></figcaption></figure>

