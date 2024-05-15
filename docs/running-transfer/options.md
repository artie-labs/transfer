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

<table><thead><tr><th width="427">Key</th><th width="102" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>outputSource</code></td><td align="center">N</td><td><p>This is the destination.<br>Supported values are currently: </p><ul><li><code>snowflake</code></li><li><code>bigquery</code></li><li><code>s3</code></li><li><code>test</code> (stdout)</li></ul></td></tr><tr><td><code>queue</code></td><td align="center">Y</td><td><p>Defaults to <code>kafka</code>.</p><p>Other valid options are <code>kafka</code> and <code>pubsub</code>. </p><p></p><p>Please check the respective sections below on what else is required.</p></td></tr><tr><td><code>reporting.sentry.dsn</code></td><td align="center">Y</td><td>DSN for Sentry alerts. If blank, will just go to stdout.</td></tr><tr><td><code>flushIntervalSeconds</code></td><td align="center">Y</td><td>Defaults to <code>10</code>.<br><br>Valid range is between <code>5 seconds</code> to <code>6 hours</code>.</td></tr><tr><td><code>bufferRows</code></td><td align="center">Y</td><td>Defaults to <code>15,000</code>.<br><br>When using BigQuery and Snowflake, there is no limit.</td></tr><tr><td><code>flushSizeKb</code></td><td align="center">Y</td><td>Defaults to <code>25mb</code>.<br><br>When the in-memory database is greater than this value, it will trigger a flush cycle.</td></tr></tbody></table>

### Kafka

```yaml
kafka:
  bootstrapServer: localhost:9092,localhost:9093
  groupID: transfer
  username: artie
  password: transfer
  enableAWSMSKIAM: false
```

#### bootstrapServer

Pass in the Kafka bootstrap server. For best practices, pass in a comma separated list of bootstrap servers to maintain high availability. This is the [same spec as Kafka](https://kafka.apache.org/documentation/#producerconfigs\_bootstrap.servers).\
**Type:** String\
**Optional:** No

#### groupID

This is the name of the Kafka consumer group. You can set to whatever you'd like. Just remember that the offsets are associated to a particular consumer group.\
**Type:** String\
**Optional:** No

#### username + password

If you'd like to use SASL/SCRAM auth, you can pass the username and password.\
**Type:** String\
**Optional:** Yes

#### enableAWSMSKIAM

Turn this on if you would like to use IAM authentication to communicate with Amazon MSK. If you enabel this, make sure to pass in `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.\
**Type:** Boolean\
**Optional:** Yes

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

<table><thead><tr><th width="386">Key</th><th width="101.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>*.topicConfigs[0].db</code></td><td align="center">N</td><td>Name of the database in destination.</td></tr><tr><td><code>*.topicConfigs[0].tableName</code></td><td align="center">Y</td><td><p>Name of the table in destination.</p><p></p><ul><li>If not provided, will use table name from event</li><li>If provided, tableName acts an override</li></ul></td></tr><tr><td><code>*.topicConfigs[0].schema</code></td><td align="center">N</td><td>Name of the schema in Snowflake.<br><br>Not needed for BigQuery.</td></tr><tr><td><code>*.topicConfigs[0].topic</code></td><td align="center">N</td><td>Name of the Kafka topic.</td></tr><tr><td><code>*.topicConfigs[0].idempotentKey</code></td><td align="center">N</td><td>Name of the column that is used for idempotency. This field is highly recommended.<br>For example: <code>updated_at</code> or another timestamp column.</td></tr><tr><td><code>*.topicConfigs[0].cdcFormat</code></td><td align="center">N</td><td><p>Name of the CDC connector (thus format) we should be expecting to parse against.<br><br>Currently, the supported values are:</p><ol><li><code>debezium.postgres</code></li><li><code>debezium.mongodb</code></li><li><code>debezium.mysql</code></li></ol></td></tr><tr><td><code>*.topicConfigs[0].cdcKeyFormat</code></td><td align="center">N</td><td>Format for what Kafka Connect will the key to be. This is called <code>key.converter</code> in the Kafka Connect properties file.<br>The supported values are: <code>org.apache.kafka.connect.storage.StringConverter</code>, <code>org.apache.kafka.connect.json.JsonConverter</code><br>If not provided, the default value will be <code>org.apache.kafka.connect.storage.StringConverter</code>. </td></tr><tr><td><code>*.topicConfigs[0].dropDeletedColumns</code></td><td align="center">Y</td><td>Defaults to <code>false</code>. <br><br>When set to <code>true</code>, Transfer will drop columns in the destination when Transfer detects that the source has dropped these columns. This column should be turned on if your organization follows standard practice around database migrations.<br><br>This is available starting <code>transfer:1.4.4</code>.</td></tr><tr><td><code>*.topicConfigs[0].softDelete</code></td><td align="center">Y</td><td>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will add an additional column called <code>__artie_delete</code> and will set the column to true instead of issuing a hard deletion. <br><br>This is available starting <code>transfer:1.4.4</code>.</td></tr><tr><td><code>*.topicConfigs[0].skippedOperations</code></td><td align="center">Y</td><td><p>Comma-separated string for Transfer to specified operations. <br><br>Valid values are:</p><ul><li>c (create)</li><li>r (replication or backfill)</li><li>u (update)</li><li>d (delete)</li></ul><p>Can be specified like: <code>c,d</code> to skip create and deletes.<br><br>This is available starting <code>transfer:2.2.3</code></p></td></tr><tr><td><p><code>*.topicConfigs[0].skipDelete</code></p><p><br>This is getting deprecated in the next Transfer version. Use <code>skippedOperations</code> instead.</p></td><td align="center">Y</td><td>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will skip the delete events.<br><br>This is available starting <code>transfer:2.0.48</code></td></tr><tr><td><code>*.topicConfigs[0].includeArtieUpdatedAt</code></td><td align="center">Y</td><td>Defaults to <code>false</code>. <br><br>When set to <code>true</code>, Transfer will emit an additional timestamp column named <code>__artie_updated_at</code> which signifies when this row was processed.<br><br>This is available starting <code>transfer:2.0.17</code></td></tr><tr><td><code>*.topicConfig[0].includeDatabaseUpdatedAt</code></td><td align="center">Y</td><td>Defaults to <code>false</code>.<br><br>When set to <code>true</code>, Transfer will emit an additional timestamp column called <code>__artie_db_updated_at</code> which signifies the database time of when the row was processed.<br><br>This is available starting <code>transfer:2.2.2+</code></td></tr><tr><td><code>*.topicConfigs[0].bigQueryPartitionSettings</code></td><td align="center">Y</td><td>Enable this to turn on BigQuery table partitioning. <br><br>This is available starting <code>transfer:2.0.24</code></td></tr></tbody></table>

#### BigQuery Partition Settings

This is the object stored under [Topic Config](options.md#topic-configs).&#x20;

```yaml
bigQueryPartitionSettings:
  partitionType: time
  partitionField: ts
  partitionBy: daily
```

#### partitionType

Type of partitioning. We currently support only time-based partitioning. The valid values right now are just `time`.\
**Type:** String\
**Optional:** Yes

#### partitionField

Which field or column is being partitioned on.\
**Type:** String\
**Optional:** Yes

#### partitionBy

This is used for time partitioning, what is the time granularity? Valid values right now are just `daily`\
**Type:** String\
**Optional:** Yes

### Google Pub/Sub

```yaml
pubsub:
  projectID: 123
  pathToCredentials: /path/to/pubsub.json
  topicConfigs:
  - { }
```

#### projectID

This is your GCP Project ID, click here to see how you can find it.[#getting-your-project-identifier](../real-time-destinations/bigquery.md#getting-your-project-identifier "mention")**Type:** String\
**Optional**: No

#### pathToCredentials

This is the path to the credentials for the service account to use. You can re-use the same credentials as BigQuery, or you can use a different service account to support use cases of cross-account transfers.\
**Type:** String\
**Optional:** No

#### topicConfigs

Follow the same convention as `kafka.topicConfigs` above.

### BigQuery

<table><thead><tr><th width="390">Key</th><th width="103.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>bigquery.pathToCredentials</code></td><td align="center">Y</td><td>Path to the credentials file for Google. <br><br>You can also directly inject <code>GOOGLE_APPLICATION_CREDENTIALS</code> ENV VAR, else Transfer will set it for you based on this value provided.</td></tr><tr><td><code>bigquery.projectID</code></td><td align="center">N</td><td>Google Cloud Project ID</td></tr><tr><td><code>bigquery.location</code></td><td align="center">Y</td><td>Location of the BigQuery dataset. <br><br>Defaults to <code>us</code>.</td></tr><tr><td><code>bigquery.defaultDataset</code></td><td align="center">N</td><td><p>The default dataset used. </p><p></p><p>This just allows us to connect to BigQuery using data source  notation (DSN). </p></td></tr><tr><td><code>bigquery.batchSize</code></td><td align="center">Y</td><td>Batch size is used to chunk the request to BigQuery's Storage API to avoid the 10 mb limit.<br><br>If this is not passed in, we will just default to <code>1,000</code>.</td></tr></tbody></table>

### Shared Transfer config

```yaml
sharedTransferConfig:
  additionalDateFormats:
    - 02/01/06 # DD/MM/YY
    - 02/01/2006 # DD/MM/YYYY
  createAllColumnsIfAvailable: true
```

#### **additionalDateFormats**

By default, Artie Transfer supports a [wide array of date formats](https://github.com/artie-labs/transfer/blob/master/lib/typing/ext/variables.go). If your layout is supported, you can specify additional ones here. If you're unsure, please refer to this [guide](https://yourbasic.org/golang/format-parse-string-time-date-example/).\
**Type:** List of layouts\
**Optional:** Yes

#### createAllColumnsIfAvailable

By default, Artie Transfer will only create the column within the destination if the column contains a not null value. You can override this behavior by setting this value to `true`.\
**Type:** Boolean\
**Optional:** Yes

### Snowflake

Please see: [snowflake.md](../real-time-destinations/snowflake.md "mention") on how to gather these values.

<table><thead><tr><th width="434">Key</th><th width="110" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>snowflake.account</code></td><td align="center">N</td><td><a href="https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#account-identifiers">Account Identifier</a></td></tr><tr><td><code>snowflake.username</code></td><td align="center">N</td><td>Snowflake username</td></tr><tr><td><code>snowflake.password</code></td><td align="center">N</td><td>Snowflake password</td></tr><tr><td><code>snowflake.warehouse</code></td><td align="center">N</td><td>Virtual warehouse name</td></tr><tr><td><code>snowflake.region</code></td><td align="center">N</td><td>Snowflake region.</td></tr></tbody></table>

### Redshift

<table><thead><tr><th width="321">Key</th><th width="106.33333333333331" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>redshift.host</code></td><td align="center">N</td><td>Host URL<br>e.g. <code>test-cluster.us-east-1.redshift.amazonaws.com</code></td></tr><tr><td><code>redshift.port</code></td><td align="center">N</td><td>-</td></tr><tr><td><code>redshift.database</code></td><td align="center">N</td><td>Namespace / Database in Redshift.</td></tr><tr><td><code>redshift.username</code></td><td align="center">N</td><td></td></tr><tr><td><code>redshift.password</code></td><td align="center">N</td><td></td></tr><tr><td><code>redshift.bucket</code></td><td align="center">N</td><td><p>Bucket for where staging files will be stored.<br></p><p></p><p><a href="https://docs.artie.com/os-tutorials/running-redshift#setting-up-s3-bucket">Click here</a> to see how to set up a S3 bucket and have it automatically purged based on expiration.</p></td></tr><tr><td><code>redshift.optionalS3Prefix</code></td><td align="center">Y</td><td><p>The prefix for S3, say bucket is <strong>foo</strong> and prefix is <strong>bar</strong>.<br></p><p>It becomes:<br>s3://foo/bar/file.txt</p></td></tr><tr><td><code>redshift.credentialsClause</code></td><td align="center">N</td><td>Redshift credentials clause to store staging files into S3. <br><br><a href="https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html">Source</a></td></tr><tr><td><code>redshift.skipLgCols</code></td><td align="center">Y</td><td>Defaults to false. <br><br>If this is passed in, Artie Transfer will mask the column value with:<br><br>1. If value is a string, <code>__artie_exceeded_value</code><br>2. if value is a struct / super,<br><code>{"key":"__artie_exceeded_value"}</code></td></tr></tbody></table>

### S3

```yaml
s3:
  optionalPrefix: foo # Files will be saved under s3://artie-transfer/foo/...
  bucket: artie-transfer
  awsAccessKeyID: AWS_ACCESS_KEY_ID
  awsSecretAccessKey: AWS_SECRET_ACCESS_KEY
```

#### optionalPrefix

Prefix after the bucket name. If this is specified, Artie Transfer will save the files under `s3://artie-transfer/optionalPrefix/...`\
**Type:** String\
Optional: Yes

#### bucket

S3 bucket name. Example: `foo`.\
**Type:** String\
**Optional:** No

#### awsAccessKeyID

The `AWS_ACCESS_KEY_ID` for the service account.\
**Type:** String\
**Optional:** No

#### awsSecretAccessKey

The `AWS_SECRET_ACCESS_KEY` for the service account.\
**Type:** String\
**Optional:** No

### Telemetry

Overview of Telemetry can be found here: [Broken link](broken-reference "mention").

<table><thead><tr><th width="429">Key</th><th width="95" align="center">Type</th><th width="102" align="center">Optional</th><th>Description</th></tr></thead><tbody><tr><td><code>telemetry.metrics</code></td><td align="center">Object</td><td align="center">Y</td><td>Parent object. See below.</td></tr><tr><td><code>telemetry.metrics.provider</code></td><td align="center">String</td><td align="center">Y</td><td>Provider to export metrics to. Transfer currently only supports: <code>datadog</code>.</td></tr><tr><td><code>telemetry.metrics.settings</code></td><td align="center">Object</td><td align="center">Y</td><td>Additional settings block, see below</td></tr><tr><td><code>telemetry.metrics.settings.tags</code></td><td align="center">Array</td><td align="center">Y</td><td>Tags that will appear for every metrics like: <code>env:production</code>, <code>company:foo</code></td></tr><tr><td><code>telemetry.metrics.settings.namespace</code></td><td align="center">String</td><td align="center">Y</td><td>Optional namespace prefix for metrics. Defaults to <code>transfer.</code> if none is provided.</td></tr><tr><td><code>telemetry.metrics.settings.addr</code></td><td align="center">String</td><td align="center">Y</td><td>Address for where the statsD agent is running. Defaults to <code>127.0.0.1:8125</code> if none is provided.</td></tr><tr><td><code>telemetry.metrics.settings.sampling</code></td><td align="center">Number</td><td align="center">Y</td><td>Percentage of data to send. Provide a number between 0 and 1. Defaults to <code>1</code> if none is provided. Refer to <a href="https://docs.datadoghq.com/metrics/custom_metrics/dogstatsd_metrics_submission/#sample-rates">this</a> for additional information.</td></tr></tbody></table>
