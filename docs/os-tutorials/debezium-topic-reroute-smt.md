---
description: >-
  In this tutorial, we will go over how to set up Debezium Topic Routing SMTs
  and how this works with Transfer.
---

# Debezium Topic Reroute SMT

### Available

After `artielabs/transfer:2.0.3`+

### Why is this useful?

By default, Debezium will map 1 topic to 1 table. There are 2 common scenarios where you'd want this to be different.

1. **A partitioned database.** You might want all the messages to show up on the same topic as opposed to one topic per partition.
2. **You don't want to manually create topics.** If you are using the pub/sub setting, Debezium does not automatically create topics. This means you will need to manually do so. This can be onerous as you will need to manually create a new topic per table.

**Debezium `application.properties`**

```properties
# Offset storage
debezium.source.offset.storage.file.filename=/tmp/foo
debezium.source.offset.flush.interval.ms=0

# Pubsub setup: https://debezium.io/documentation/reference/stable/operations/debezium-server.html#_google_cloud_pubsub
debezium.sink.type=pubsub
debezium.sink.pubsub.project.id=artie-labs
debezium.sink.pubsub.ordering.enabled=true

# Postgres
debezium.source.snapshot.mode=initial
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.database.hostname=postgres
debezium.source.database.port=5432
debezium.source.database.user=postgres
debezium.source.database.password=postgres
debezium.source.database.dbname=postgres
debezium.source.topic.prefix=dbserver1
# Syncing customers, orders and products table.
debezium.source.table.include.list=inventory.customers,inventory.orders,inventory.products,inventory.users
debezium.source.plugin.name=pgoutput

# Re-route them all to the same topic.
debezium.transforms=Reroute
debezium.transforms.Reroute.type=io.debezium.transforms.ByLogicalTableRouter
# Matches all the tables under the inventory schema
debezium.transforms.Reroute.topic.regex=(.*).inventory(.*)
# Becomes, topicPrefix.schema.all_tables => dbserver1.inventory.all_tables
debezium.transforms.Reroute.topic.replacement=$1.inventory.all_tables
```

Transfer `config.yaml`

```yaml
outputSource: bigquery
queue: pubsub
flushIntervalSeconds: 30
bufferRows: 99999
pubsub:
  projectID: artie-labs
  pathToCredentials: /tmp/bq.json
  topicConfigs:
    - db: customers
      schema: public
      topic: dbserver1.inventory.all_tables
      cdcFormat: debezium.postgres.wal2json
      cdcKeyFormat: org.apache.kafka.connect.json.JsonConverter

bigquery:
  pathToCredentials: /tmp/bq.json
  projectID: artie-labs
  defaultDataset: fake
```

