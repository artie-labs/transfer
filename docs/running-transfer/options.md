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

<table><thead><tr><th width="427">Key</th><th width="102">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>outputSource</code></td><td>N</td><td>This is the destination.<br>Supported values are currently: <code>snowflake_stage</code>, <code>snowflake</code>, <code>test</code>, <code>bigquery</code><br><br>If Snowflake or BigQuery is filled out, please see the respective section for what is required.</td></tr><tr><td><code>queue</code></td><td>Y</td><td><p>Defaults to <code>kafka</code>.</p><p>Other valid options are <code>kafka</code> and <code>pubsub</code>. </p><p></p><p>Please check the respective sections below on what else is required.</p></td></tr><tr><td><code>reporting.sentry.dsn</code></td><td>Y</td><td>DSN for Sentry alerts. If blank, will just go to standard out.</td></tr><tr><td><code>flushIntervalSeconds</code></td><td>Y</td><td>Defaults to <code>10</code>.<br><br>Valid range is between <code>5 seconds</code> to <code>6 hours</code>.</td></tr><tr><td><code>bufferRows</code></td><td>Y</td><td><p>Defaults to <code>15000</code>.<br><br>When using BigQuery and Snowflake stages, there is no limit.<br></p><p>For Snowflake, the valid range is between <code>5-15000</code></p></td></tr><tr><td><code>flushSizeKb</code></td><td>Y</td><td>Defaults to <code>25mb</code>.<br><br>When the in-memory database is greater than this value, it will trigger a flush cycle.</td></tr></tbody></table>

### Kafka

<table><thead><tr><th width="433">Key</th><th width="110.33333333333331">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>kafka.bootstrapServer</code></td><td>N</td><td>Comma separated list of bootsrap servers.<br><br>Following the <a href="https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers">same spec as Kafka</a>.<br><br>Example:<br><code>localhost:9092</code><br><br><code>host1:port1,host2:port2</code></td></tr><tr><td><code>kafka.groupID</code></td><td>N</td><td>Consumer group ID</td></tr><tr><td><code>kafka.username</code></td><td>Y</td><td>Username (Transfer correctly only supports Plain SASL or no authentication).</td></tr><tr><td><code>kafka.password</code></td><td>Y</td><td>Password</td></tr><tr><td><code>kafka.enableAWSMKSIAM</code></td><td>Y</td><td>Defaults to <code>false</code>, turn this on if you want to use IAM authentication for communicating with Amazon MSK. <br><br>Make sure to unset username and password and provide: <code>AWS_REGION</code>, <code>AWS_ACCESS_KEY_ID</code>, <code>AWS_SECRET_ACCESS_KEY</code></td></tr></tbody></table>

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

<table><thead><tr><th width="436">Key</th><th width="108.33333333333331">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>*.topicConfigs[0].db</code></td><td>N</td><td>Name of the database in destination.</td></tr><tr><td><code>*.topicConfigs[0].tableName</code></td><td>Y</td><td>Optional. Name of the table in destination.<br><br>* If not provided, we'll use the table name from the event.<br><br>* If provided, <code>tableName</code> acts as an override.</td></tr><tr><td><code>*.topicConfigs[0].schema</code></td><td>N</td><td>Name of the schema in <strong>Snowflake</strong> (required).<br><br>Not needed for BigQuery.</td></tr><tr><td><code>*.topicConfigs[0].topic</code></td><td>N</td><td>Name of the Kafka topic.</td></tr><tr><td><code>*.topicConfigs[0].idempotentKey</code></td><td>N</td><td>Name of the column that is used for idempotency. This field is highly recommended.<br>For example: <code>updated_at</code> or another timestamp column.</td></tr><tr><td><code>*.topicConfigs[0].cdcFormat</code></td><td>N</td><td><p>Name of the CDC connector (thus format) we should be expecting to parse against.<br>Currently, the supported values are:</p><ol><li><code>debezium.postgres</code></li><li><code>debezium.mongodb</code></li><li><code>debezium.mysql</code></li></ol></td></tr><tr><td><code>*.topicConfigs[0].cdcKeyFormat</code></td><td>N</td><td>Format for what Kafka Connect will the key to be. This is called <code>key.converter</code> in the Kafka Connect properties file.<br>The supported values are: <code>org.apache.kafka.connect.storage.StringConverter</code>, <code>org.apache.kafka.connect.json.JsonConverter</code><br>If not provided, the default value will be <code>org.apache.kafka.connect.storage.StringConverter</code>. </td></tr><tr><td><code>*.topicConfigs[0].dropDeletedColumns</code></td><td>Y</td><td>Defaults to <code>false</code>. <br><br>When set to <code>true</code>, Transfer will drop columns in the destination when Transfer detects that the source has dropped these columns. This column should be turned on if your organization follows standard practice around database migrations.<br><br>This is available starting <code>transfer:1.4.4</code>.</td></tr><tr><td><code>*.topicConfigs[0].softDelete</code></td><td>Y</td><td>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will add an additional column called <code>__artie_delete</code> and will set the column to true instead of issuing a hard deletion. <br><br>This is available starting <code>transfer:1.4.4</code>.</td></tr></tbody></table>

### Google Pub/Sub

<table><thead><tr><th width="435.3333333333333">Key</th><th width="107">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>pubsub.projectID</code></td><td>N</td><td>This is your GCP project ID. See <a data-mention href="../real-time-destinations/bigquery.md#getting-your-project-identifier">#getting-your-project-identifier</a>on how to find it.</td></tr><tr><td><code>pubsub.pathToCredentials</code></td><td>N</td><td>Note: Transfer can support different credentials for BigQuery and Pub/Sub. Such that you can consume from one project and write to BQ on another.</td></tr><tr><td><code>pubsub.topicConfigs</code></td><td>N</td><td>The topicConfigs here follows the same convention as <code>kafka.topicConfigs</code>. Please see above.</td></tr></tbody></table>

### BigQuery

<table><thead><tr><th width="433">Key</th><th width="106.33333333333331">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>bigquery.pathToCredentials</code></td><td>Y</td><td>Path to the credentials file for Google. <br><br>You can also directly inject <code>GOOGLE_APPLICATION_CREDENTIALS</code> ENV VAR, else Transfer will set it for you based on this value provided.</td></tr><tr><td><code>bigquery.projectID</code></td><td>N</td><td>Google Cloud Project ID</td></tr><tr><td><code>bigquery.defaultDataset</code></td><td>N</td><td><p>The default dataset used. </p><p></p><p>This just allows us to connect to BigQuery using data source  notation (DSN). </p></td></tr></tbody></table>

### Snowflake

Please see: [snowflake.md](../real-time-destinations/snowflake.md "mention") on how to gather these values.

<table><thead><tr><th width="434">Key</th><th width="110">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>snowflake.account</code></td><td>N</td><td>Snowflake <a href="https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#account-identifiers">Account Identifier</a></td></tr><tr><td><code>snowflake.username</code></td><td>N</td><td>Snowflake username</td></tr><tr><td><code>snowflake.password</code></td><td>N</td><td>Snowflake password</td></tr><tr><td><code>snowflake.warehouse</code></td><td>N</td><td>Snowflake virtual warehouse name</td></tr><tr><td><code>snowflake.region</code></td><td>N</td><td>Snowflake region.</td></tr></tbody></table>

### Telemetry

Overview of Telemetry can be found here: [Broken link](broken-reference "mention").

<table><thead><tr><th width="429">Key</th><th width="95">Type</th><th width="102">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>telemetry.metrics</code></td><td>Object</td><td>Y</td><td>Parent object. See below.</td></tr><tr><td><code>telemetry.metrics.provider</code></td><td>String</td><td>Y</td><td>Provider to export metrics to. Transfer currently only supports: <code>datadog</code>.</td></tr><tr><td><code>telemetry.metrics.settings</code></td><td>Object</td><td>Y</td><td>Additional settings block, see below</td></tr><tr><td><code>telemetry.metrics.settings.tags</code></td><td>Array</td><td>Y</td><td>Tags that will appear for every metrics like: <code>env:production</code>, <code>company:foo</code></td></tr><tr><td><code>telemetry.metrics.settings.namespace</code></td><td>String</td><td>Y</td><td>Optional namespace prefix for metrics. Defaults to <code>transfer.</code> if none is provided.</td></tr><tr><td><code>telemetry.metrics.settings.addr</code></td><td>String</td><td>Y</td><td>Address for where the statsD agent is running. Defaults to <code>127.0.0.1:8125</code> if none is provided.</td></tr><tr><td><code>telemetry.metrics.settings.sampling</code></td><td>Number</td><td>Y</td><td>Percentage of data to send. Provide a number between 0 and 1. Defaults to <code>1</code> if none is provided. Refer to <a href="https://docs.datadoghq.com/metrics/custom_metrics/dogstatsd_metrics_submission/#sample-rates">this</a> for additional information.</td></tr></tbody></table>
