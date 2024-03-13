# Examples

## Introduction

Below, you can see sample configuration files to describe different workloads. To see all the available settings, please see the [options.md](options.md "mention") page.

## Postgres options

### Postgres -> Snowflake

```yaml
outputSource: snowflake
kafka:
  bootstrapServer: kafka:9092
  groupID: group_abc
  topicConfigs:
    - db: shop
      tableName: customers
      schema: inventory
      topic: "dbserver1.inventory.customers"
      cdcFormat: debezium.postgres.wal2json
      cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter
snowflake:
  account: ACCOUNT_ID
  username: USER
  password: PASSWORD
  warehouse: DWH_NAME
  region: us-east-2.aws
```

### Postgres -> BigQuery

```yaml
outputSource: bigquery
kafka:
  bootstrapServer: kafka:9092
  groupID: group_abc
  topicConfigs:
    - db: shop
      tableName: customers
      schema: inventory
      topic: "dbserver1.inventory.customers"
      cdcFormat: debezium.postgres.wal2json
      cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter
bigquery:
  pathToCredentials: PATH_TO_CREDENTIALS
  projectID: PROJECT_ID
  defaultDataset: DEFAULT_DATASET
```

## MongoDB options

### MongoDB -> Snowflake

```yaml
outputSource: snowflake
kafka:
  bootstrapServer: kafka:9092
  groupID: group_abc
  topicConfigs:
    - db: shop
      tableName: customers
      schema: inventory
      topic: "dbserver1.inventory.customers"
      cdcFormat: debezium.mongodb
      cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter
snowflake:
  account: ACCOUNT_ID
  username: USER
  password: PASSWORD
  warehouse: DWH_NAME
  region: us-east-2.aws
```

### MongoDB -> BigQuery

```yaml
outputSource: bigquery
kafka:
  bootstrapServer: kafka:9092
  groupID: group_abc
  topicConfigs:
    - db: shop
      tableName: customers
      schema: inventory
      topic: "dbserver1.inventory.customers"
      cdcFormat: debezium.mongodb
      cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter
bigquery:
  pathToCredentials: PATH_TO_CREDENTIALS
  projectID: PROJECT_ID
  defaultDataset: DEFAULT_DATASET
```

## Optional Blocks

### Error Reporting to Sentry

If you provide your Sentry DSN; Artie Transfer will automatically report any errors during processing into your Sentry project.&#x20;

Visit this [link](https://docs.sentry.io/product/sentry-basics/dsn-explainer/) to see how you can find your Sentry DSN.

```yaml
reporting:
  sentry:
    dsn: https://docs.sentry.io/product/sentry-basics/dsn-explainer/    
```

Transfer is using a vendor neutral logging library, so file a [feature request](https://github.com/artie-labs/transfer/issues/new) if you use Rollbar, or another vendor!&#x20;

### Telemetry

Visit the [Broken link](broken-reference "mention") page to see all the metrics that Transfer emits.

```yaml
telemetry:
  metrics:
    provider: datadog
    settings:
      tags:
       - env:production
       - customer:artie.so
      namespace: "transfer."
      addr: "127.0.0.1:8125"
```

