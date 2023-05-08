---
description: >-
  This page describes the available configuration settings for Artie Transfer to
  use.
---

# Options

Below, these are the various options that can be specified within a configuration file. Once it has been created, you can run Artie Transfer like this:

```bash
/transfer -c /path/to/config.yaml
```

_Note: Keys here are formatted in dot notation for readability purposes, please ensure that the proper nesting is done when writing this into your configuration file. To see sample configuration files, visit the_ [examples.md](examples.md "mention") page.

| Key                    | Optional | Description                                                                                                                                                                                                                                |
| ---------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `outputSource`         | N        | <p>This is the destination.<br>Supported values are currently: <code>snowflake</code>, <code>test</code>, <code>bigquery</code><br><br>If Snowflake or BigQuery is filled out, please see the respective section for what is required.</p> |
| `queue`                | Y        | <p>Defaults to <code>kafka</code>.</p><p>Other valid options are <code>kafka</code> and <code>pubsub</code>. </p><p></p><p>Please check the respective sections below on what else is required.</p>                                        |
| `reporting.sentry.dsn` | Y        | DSN for Sentry alerts. If blank, will just go to standard out.                                                                                                                                                                             |
| `flushIntervalSeconds` | Y        | <p>Defaults to <code>10</code>.<br><br>Valid range is between <code>5 seconds</code> to <code>6 hours</code>.</p>                                                                                                                          |
| `bufferRows`           | Y        | <p>Defaults to <code>15000</code>.<br><br>Valid range is between <code>5-15000</code></p>                                                                                                                                                  |
| `flushSizeKb`          | Y        | <p>Defaults to <code>15 mb</code>.<br><br>When the in-memory database is greater than this value, it will trigger a flush cycle.</p>                                                                                                       |

### Kafka

| Key                     | Optional | Description                                                                                                                                                                                                                                                                            |
| ----------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kafka.bootstrapServer` | N        | <p>URL to the Kafka broker, including the port number.<br><br>Example:<br><code>localhost:9092</code></p>                                                                                                                                                                              |
| `kafka.groupID`         | N        | Consumer group ID                                                                                                                                                                                                                                                                      |
| `kafka.username`        | Y        | Username (Transfer correctly only supports Plain SASL or no authentication).                                                                                                                                                                                                           |
| `kafka.password`        | Y        | Password                                                                                                                                                                                                                                                                               |
| `kafka.enableAWSMKSIAM` | Y        | <p>Defaults to <code>false</code>, turn this on if you want to use IAM authentication for communicating with Amazon MSK. <br><br>Make sure to unset username and password and provide: <code>AWS_REGION</code>, <code>AWS_ACCESS_KEY_ID</code>, <code>AWS_SECRET_ACCESS_KEY</code></p> |

### Topic Configs

`TopicConfigs` are used at the table level and store configurations like:

* Destination's database, schema and table name.
* What does the data format look like? Is there an idempotent key?
* Whether it should do row based soft deletion or not.
* Whether it should drop deleted columns or not.

These are stored in this particular fashion. See [examples.md](examples.md "mention") for more details.

```yaml
kafka:
    topicConfigs:
    - { }
    - { }
# OR as
pubsub:
    topicConfigs:
    - { }
    - { }
```

| Key                                    | Optional | Description                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `*.topicConfigs[0].db`                 | N        | Name of the database in destination.                                                                                                                                                                                                                                                                                                                                                                        |
| `*.topicConfigs[0].tableName`          | N        | Name of the table in destination.                                                                                                                                                                                                                                                                                                                                                                           |
| `*.topicConfigs[0].schema`             | N        | <p>Name of the schema in <strong>Snowflake</strong> (required).<br><br>Not needed for BigQuery.</p>                                                                                                                                                                                                                                                                                                         |
| `*.topicConfigs[0].topic`              | N        | Name of the Kafka topic.                                                                                                                                                                                                                                                                                                                                                                                    |
| `*.topicConfigs[0].idempotentKey`      | N        | <p>Name of the column that is used for idempotency. This field is highly recommended.<br>For example: <code>updated_at</code> or another timestamp column.</p>                                                                                                                                                                                                                                              |
| `*.topicConfigs[0].cdcFormat`          | N        | <p>Name of the CDC connector (thus format) we should be expecting to parse against.<br>Currently, the supported values are:</p><ol><li><code>debezium.postgres</code></li><li><code>debezium.mongodb</code></li><li><code>debezium.mysql</code></li></ol>                                                                                                                                                   |
| `*.topicConfigs[0].cdcKeyFormat`       | N        | <p>Format for what Kafka Connect will the key to be. This is called <code>key.converter</code> in the Kafka Connect properties file.<br>The supported values are: <code>org.apache.kafka.connect.storage.StringConverter</code>, <code>org.apache.kafka.connect.json.JsonConverter</code><br>If not provided, the default value will be <code>org.apache.kafka.connect.storage.StringConverter</code>. </p> |
| `*.topicConfigs[0].dropDeletedColumns` | Y        | <p>Defaults to <code>false</code>. <br><br>When set to <code>true</code>, Transfer will drop columns in the destination when Transfer detects that the source has dropped these columns. This column should be turned on if your organization follows standard practice around database migrations.<br><br>This is available starting <code>transfer:1.4.4</code>.</p>                                      |
| `*.topicConfigs[0].softDelete`         | Y        | <p>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will add an additional column called <code>__artie_delete</code> and will set the column to true instead of issuing a hard deletion. <br><br>This is available starting <code>transfer:1.4.4</code>.</p>                                                                                                                  |

### Google Pub/Sub

| Key                        | Optional | Description                                                                                                                                                            |
| -------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pubsub.projectID`         | N        | This is your GCP project ID. See [#getting-your-project-identifier](../real-time-destinations/bigquery.md#getting-your-project-identifier "mention")on how to find it. |
| `pubsub.pathToCredentials` | N        | Note: Transfer can support different credentials for BigQuery and Pub/Sub. Such that you can consume from one project and write to BQ on another.                      |
| `pubsub.topicConfigs`      | N        | The topicConfigs here follows the same convention as `kafka.topicConfigs`. Please see above.                                                                           |

### BigQuery

| Key                          | Optional | Description                                                                                                                                                                                               |
| ---------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bigquery.pathToCredentials` | Y        | <p>Path to the credentials file for Google. <br><br>You can also directly inject <code>GOOGLE_APPLICATION_CREDENTIALS</code> ENV VAR, else Transfer will set it for you based on this value provided.</p> |
| `bigquery.projectID`         | N        | Google Cloud Project ID                                                                                                                                                                                   |
| `bigquery.defaultDataset`    | N        | <p>The default dataset used. </p><p></p><p>This just allows us to connect to BigQuery using data source  notation (DSN). </p>                                                                             |

### Snowflake

Please see: [snowflake.md](../real-time-destinations/snowflake.md "mention") on how to gather these values.

| Key                   | Optional | Description                                                                                                                |
| --------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------- |
| `snowflake.account`   | N        | Snowflake [Account Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#account-identifiers) |
| `snowflake.username`  | N        | Snowflake username                                                                                                         |
| `snowflake.password`  | N        | Snowflake password                                                                                                         |
| `snowflake.warehouse` | N        | Snowflake virtual warehouse name                                                                                           |
| `snowflake.region`    | N        | Snowflake region.                                                                                                          |

### Telemetry

Overview of Telemetry can be found here: [Broken link](broken-reference "mention").

| Key                                    | Type   | Optional | Description                                                                                                                                                                                                                                     |
| -------------------------------------- | ------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry.metrics`                    | Object | Y        | Parent object. See below.                                                                                                                                                                                                                       |
| `telemetry.metrics.provider`           | String | Y        | Provider to export metrics to. Transfer currently only supports: `datadog`.                                                                                                                                                                     |
| `telemetry.metrics.settings`           | Object | Y        | Additional settings block, see below                                                                                                                                                                                                            |
| `telemetry.metrics.settings.tags`      | Array  | Y        | Tags that will appear for every metrics like: `env:production`, `company:foo`                                                                                                                                                                   |
| `telemetry.metrics.settings.namespace` | String | Y        | Optional namespace prefix for metrics. Defaults to `transfer.` if none is provided.                                                                                                                                                             |
| `telemetry.metrics.settings.addr`      | String | Y        | Address for where the statsD agent is running. Defaults to `127.0.0.1:8125` if none is provided.                                                                                                                                                |
| `telemetry.metrics.settings.sampling`  | Number | Y        | Percentage of data to send. Provide a number between 0 and 1. Defaults to `1` if none is provided. Refer to [this](https://docs.datadoghq.com/metrics/custom\_metrics/dogstatsd\_metrics\_submission/#sample-rates) for additional information. |
