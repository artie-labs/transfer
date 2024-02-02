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

<table><thead><tr><th width="427">Key</th><th width="102" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>outputSource</code></td><td align="center">N</td><td><p>This is the destination.<br>Supported values are currently: </p><ul><li><code>snowflake</code></li><li><code>bigquery</code></li><li><code>s3</code></li><li><code>test</code> (logs to stdout)</li></ul></td></tr><tr><td><code>queue</code></td><td align="center">Y</td><td><p>Defaults to <code>kafka</code>.</p><p>Other valid options are <code>kafka</code> and <code>pubsub</code>. </p><p></p><p>Please check the respective sections below on what else is required.</p></td></tr><tr><td><code>reporting.sentry.dsn</code></td><td align="center">Y</td><td>DSN for Sentry alerts. If blank, will just go to standard out.</td></tr><tr><td><code>flushIntervalSeconds</code></td><td align="center">Y</td><td>Defaults to <code>10</code>.<br><br>Valid range is between <code>5 seconds</code> to <code>6 hours</code>.</td></tr><tr><td><code>bufferRows</code></td><td align="center">Y</td><td><p>Defaults to <code>15000</code>.<br><br>When using BigQuery and Snowflake stages, there is no limit.<br></p><p>For Snowflake, the valid range is between <code>5-15000</code></p></td></tr><tr><td><code>flushSizeKb</code></td><td align="center">Y</td><td>Defaults to <code>25mb</code>.<br><br>When the in-memory database is greater than this value, it will trigger a flush cycle.</td></tr></tbody></table>

### Kafka

<table><thead><tr><th width="433">Key</th><th width="110.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>kafka.bootstrapServer</code></td><td align="center">N</td><td>Comma separated list of bootsrap servers.<br><br>Following the <a href="https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers">same spec as Kafka</a>.<br><br>Example:<br><code>localhost:9092</code><br><br><code>host1:port1,host2:port2</code></td></tr><tr><td><code>kafka.groupID</code></td><td align="center">N</td><td>Consumer group ID</td></tr><tr><td><code>kafka.username</code></td><td align="center">Y</td><td>Username (Transfer correctly only supports Plain SASL or no authentication).</td></tr><tr><td><code>kafka.password</code></td><td align="center">Y</td><td>Password</td></tr><tr><td><code>kafka.enableAWSMKSIAM</code></td><td align="center">Y</td><td>Defaults to <code>false</code>, turn this on if you want to use IAM authentication for communicating with Amazon MSK. <br><br>Make sure to unset username and password and provide: <code>AWS_REGION</code>, <code>AWS_ACCESS_KEY_ID</code>, <code>AWS_SECRET_ACCESS_KEY</code></td></tr></tbody></table>

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

<table><thead><tr><th width="469">Key</th><th width="101.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>*.topicConfigs[0].db</code></td><td align="center">N</td><td>Name of the database in destination.</td></tr><tr><td><code>*.topicConfigs[0].tableName</code></td><td align="center">Y</td><td>Optional. Name of the table in destination.<br><br>* If not provided, we'll use the table name from the event.<br><br>* If provided, <code>tableName</code> acts as an override.</td></tr><tr><td><code>*.topicConfigs[0].schema</code></td><td align="center">N</td><td>Name of the schema in <strong>Snowflake</strong> (required).<br><br>Not needed for BigQuery.</td></tr><tr><td><code>*.topicConfigs[0].topic</code></td><td align="center">N</td><td>Name of the Kafka topic.</td></tr><tr><td><code>*.topicConfigs[0].idempotentKey</code></td><td align="center">N</td><td>Name of the column that is used for idempotency. This field is highly recommended.<br>For example: <code>updated_at</code> or another timestamp column.</td></tr><tr><td><code>*.topicConfigs[0].cdcFormat</code></td><td align="center">N</td><td><p>Name of the CDC connector (thus format) we should be expecting to parse against.<br>Currently, the supported values are:</p><ol><li><code>debezium.postgres</code></li><li><code>debezium.mongodb</code></li><li><code>debezium.mysql</code></li></ol></td></tr><tr><td><code>*.topicConfigs[0].cdcKeyFormat</code></td><td align="center">N</td><td>Format for what Kafka Connect will the key to be. This is called <code>key.converter</code> in the Kafka Connect properties file.<br>The supported values are: <code>org.apache.kafka.connect.storage.StringConverter</code>, <code>org.apache.kafka.connect.json.JsonConverter</code><br>If not provided, the default value will be <code>org.apache.kafka.connect.storage.StringConverter</code>. </td></tr><tr><td><code>*.topicConfigs[0].dropDeletedColumns</code></td><td align="center">Y</td><td>Defaults to <code>false</code>. <br><br>When set to <code>true</code>, Transfer will drop columns in the destination when Transfer detects that the source has dropped these columns. This column should be turned on if your organization follows standard practice around database migrations.<br><br>This is available starting <code>transfer:1.4.4</code>.</td></tr><tr><td><code>*.topicConfigs[0].softDelete</code></td><td align="center">Y</td><td>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will add an additional column called <code>__artie_delete</code> and will set the column to true instead of issuing a hard deletion. <br><br>This is available starting <code>transfer:1.4.4</code>.</td></tr><tr><td><code>*.topicConfigs[0].skippedOperations</code></td><td align="center">Y</td><td>Comma-separated string for Transfer to specified operations. <br><br>Valid values are:<br>* c - create<br>* r - replication / backfill<br>* u - update<br>* d - delete<br><br>Can be specified like: <code>c,d</code> to skip create and deletes.</td></tr><tr><td><code>*.topicConfigs[0].skipDelete</code><br><br>This is getting deprecated in the next Transfer version. Use <code>skippedOperations</code> instead.</td><td align="center">Y</td><td>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will skip the delete events.<br><br>This is available starting <code>transfer:2.0.48</code></td></tr><tr><td><code>*.topicConfigs[0].includeArtieUpdatedAt</code></td><td align="center">Y</td><td>Defaults to <code>false</code>. <br><br>When set to <code>true</code>, Transfer will emit an additional timestamp column named <code>__artie_updated_at</code> which signifies when this row was processed.<br><br>This is available starting <code>transfer:2.0.17</code></td></tr><tr><td><code>*.topicConfig[0].includeDatabaseUpdatedAt</code></td><td align="center">Y</td><td>Defaults to false.<br><br>When set to true, Transfer will emit an additional timestamp column called <code>__artie_db_updated_at</code> which signifies the database time of when the row was processed.<br><br>This is available starting <code>transfer:2.2.2+</code></td></tr><tr><td><code>*.topicConfigs[0].bigQueryPartitionSettings</code></td><td align="center">Y</td><td>Enable this to turn on BigQuery table partitioning. <br><br>This is available starting <code>transfer:2.0.24</code></td></tr></tbody></table>

#### BigQuery Partition Settings

This is the object stored under [Topic Config](options.md#topic-configs).&#x20;

_Example_

```yaml
bigQueryPartitionSettings:
  partitionType: time
  partitionField: ts
  partitionBy: daily
```

<table><thead><tr><th width="380.3333333333333">Key</th><th width="99" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td>partitionType</td><td align="center">N</td><td>Type of partitioning. Currently, we support only time-based partitioning.<br><br>Valid values right now are just <code>time</code></td></tr><tr><td>partitionField</td><td align="center">N</td><td>Which field or column is being partitioned on.</td></tr><tr><td>partitionBy</td><td align="center">N</td><td>This is used for time partitioning, what is the time granularity?<br><br>Valid values right now are just <code>daily</code></td></tr></tbody></table>



### Google Pub/Sub

<table><thead><tr><th width="435.3333333333333">Key</th><th width="107" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>pubsub.projectID</code></td><td align="center">N</td><td>This is your GCP project ID. See <a data-mention href="../real-time-destinations/bigquery.md#getting-your-project-identifier">#getting-your-project-identifier</a>on how to find it.</td></tr><tr><td><code>pubsub.pathToCredentials</code></td><td align="center">N</td><td>Note: Transfer can support different credentials for BigQuery and Pub/Sub. Such that you can consume from one project and write to BQ on another.</td></tr><tr><td><code>pubsub.topicConfigs</code></td><td align="center">N</td><td>The topicConfigs here follows the same convention as <code>kafka.topicConfigs</code>. Please see above.</td></tr></tbody></table>

### BigQuery

<table><thead><tr><th width="433">Key</th><th width="106.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>bigquery.pathToCredentials</code></td><td align="center">Y</td><td>Path to the credentials file for Google. <br><br>You can also directly inject <code>GOOGLE_APPLICATION_CREDENTIALS</code> ENV VAR, else Transfer will set it for you based on this value provided.</td></tr><tr><td><code>bigquery.projectID</code></td><td align="center">N</td><td>Google Cloud Project ID</td></tr><tr><td><code>bigquery.location</code></td><td align="center">Y</td><td>Location of the BigQuery dataset. <br><br>Defaults to <code>us</code>.</td></tr><tr><td><code>bigquery.defaultDataset</code></td><td align="center">N</td><td><p>The default dataset used. </p><p></p><p>This just allows us to connect to BigQuery using data source  notation (DSN). </p></td></tr><tr><td><code>bigquery.batchSize</code></td><td align="center">Y</td><td>Batch size is used to chunk the request to BigQuery's Storage API to avoid the 10 mb limit.<br><br>If this is not passed in, we will just default to <code>1000</code>.</td></tr></tbody></table>

### Shared Transfer config



<table><thead><tr><th width="425.3333333333333">Key</th><th width="101" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>sharedTransferConfig.additionalDateFormats</code></td><td align="center">Y</td><td><p>You can specify additional date formats if they are <a href="https://github.com/artie-labs/transfer/blob/master/lib/typing/ext/variables.go">not already supported</a>.</p><p></p><p>Example:</p><pre><code>sharedTransferConfig:
  additionalDateFormats:
    - 02/01/06 # DD/MM/YY
    - 02/01/2006 # DD/MM/YYYY
</code></pre><p>If you are unsure, refer to this <a href="https://yourbasic.org/golang/format-parse-string-time-date-example/">guide</a>. </p></td></tr><tr><td><code>sharedTransferConfig.createAllColumnsIfAvailable</code></td><td align="center">Y</td><td><p>Boolean field.</p><p><br>If this is set <code>true</code>, it will create columns even if the value is <code>NULL</code>.</p></td></tr></tbody></table>

### Shared destination config

<table><thead><tr><th width="423.3333333333333">Key</th><th width="99">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>sharedDestinationConfig.uppercaseEscapedNames</code></td><td>Y</td><td>Defaults to <code>false</code>. <br><br>By enabling <a data-footnote-ref href="#user-content-fn-1">t</a>his, the escaped value will be in upper case for both table and column names.</td></tr></tbody></table>

### Snowflake

Please see: [snowflake.md](../real-time-destinations/snowflake.md "mention") on how to gather these values.

<table><thead><tr><th width="434">Key</th><th width="110" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>snowflake.account</code></td><td align="center">N</td><td>Snowflake <a href="https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#account-identifiers">Account Identifier</a></td></tr><tr><td><code>snowflake.username</code></td><td align="center">N</td><td>Snowflake username</td></tr><tr><td><code>snowflake.password</code></td><td align="center">N</td><td>Snowflake password</td></tr><tr><td><code>snowflake.warehouse</code></td><td align="center">N</td><td>Snowflake virtual warehouse name</td></tr><tr><td><code>snowflake.region</code></td><td align="center">N</td><td>Snowflake region.</td></tr></tbody></table>

### Redshift

<table><thead><tr><th width="321">Key</th><th width="106.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>redshift.host</code></td><td align="center">N</td><td>Host URL<br>e.g. <code>test-cluster.us-east-1.redshift.amazonaws.com</code></td></tr><tr><td><code>redshift.port</code></td><td align="center">N</td><td>-</td></tr><tr><td><code>redshift.database</code></td><td align="center">N</td><td>Namespace / Database in Redshift.</td></tr><tr><td><code>redshift.username</code></td><td align="center">N</td><td></td></tr><tr><td><code>redshift.password</code></td><td align="center">N</td><td></td></tr><tr><td><code>redshift.bucket</code></td><td align="center">N</td><td>Bucket for where staging files will be stored.<br><br><a href="https://docs.artie.so/tutorials/running-redshift#setting-up-s3-bucket">Click here</a> to see how to set up a S3 bucket and have it automatically purged based on expiration.</td></tr><tr><td><code>redshift.optionalS3Prefix</code></td><td align="center">Y</td><td><p>The prefix for S3, say bucket is <strong>foo</strong> and prefix is <strong>bar</strong>.<br></p><p>It becomes:<br>s3://foo/bar/file.txt</p></td></tr><tr><td><code>redshift.credentialsClause</code></td><td align="center">N</td><td>Redshift credentials clause to store staging files into S3. <br><br><a href="https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html">Source</a></td></tr><tr><td><code>redshift.skipLgCols</code></td><td align="center">Y</td><td>Defaults to false. <br><br>If this is passed in, Artie Transfer will mask the column value with:<br><br>1. If value is a string, <code>__artie_exceeded_value</code><br>2. if value is a struct / super,<br><code>{"key":"__artie_exceeded_value"}</code></td></tr></tbody></table>

### S3

<table><thead><tr><th width="300">Key</th><th width="190.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>s3.optionalPrefix</code></td><td align="center">Y</td><td>Prefix after the bucket name.</td></tr><tr><td><code>s3.bucket</code></td><td align="center">N</td><td>S3 bucket name</td></tr><tr><td><code>s3.awsAccessKeyID</code></td><td align="center">N</td><td>The <code>AWS_ACCESS_KEY_ID</code> for the service account.</td></tr><tr><td><code>s3.awsSecretAccessKey</code></td><td align="center">N</td><td>The <code>AWS_SECRET_ACCESS_KEY</code> for the service account.</td></tr></tbody></table>

### Telemetry

Overview of Telemetry can be found here: [Broken link](broken-reference "mention").

<table><thead><tr><th width="429">Key</th><th width="95" align="center">Type</th><th width="102" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>telemetry.metrics</code></td><td align="center">Object</td><td align="center">Y</td><td>Parent object. See below.</td></tr><tr><td><code>telemetry.metrics.provider</code></td><td align="center">String</td><td align="center">Y</td><td>Provider to export metrics to. Transfer currently only supports: <code>datadog</code>.</td></tr><tr><td><code>telemetry.metrics.settings</code></td><td align="center">Object</td><td align="center">Y</td><td>Additional settings block, see below</td></tr><tr><td><code>telemetry.metrics.settings.tags</code></td><td align="center">Array</td><td align="center">Y</td><td>Tags that will appear for every metrics like: <code>env:production</code>, <code>company:foo</code></td></tr><tr><td><code>telemetry.metrics.settings.namespace</code></td><td align="center">String</td><td align="center">Y</td><td>Optional namespace prefix for metrics. Defaults to <code>transfer.</code> if none is provided.</td></tr><tr><td><code>telemetry.metrics.settings.addr</code></td><td align="center">String</td><td align="center">Y</td><td>Address for where the statsD agent is running. Defaults to <code>127.0.0.1:8125</code> if none is provided.</td></tr><tr><td><code>telemetry.metrics.settings.sampling</code></td><td align="center">Number</td><td align="center">Y</td><td>Percentage of data to send. Provide a number between 0 and 1. Defaults to <code>1</code> if none is provided. Refer to <a href="https://docs.datadoghq.com/metrics/custom_metrics/dogstatsd_metrics_submission/#sample-rates">this</a> for additional information.</td></tr></tbody></table>

[^1]: 
