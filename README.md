<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201717557-17c79b66-2303-4141-bea2-87382fb02613.png" />
  <h3>Transfer by Artie</h3>
  <p>⚡️ Blazing fast data replication between OLTP and OLAP databases ⚡️</p>
  <b><a target="_blank" href="https://artie.so" >Learn more »</a></b>
</div>
<br/>

[![Go tests](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg)](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml) [![ELv2](https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg)](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt) [<img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack">](https://join.slack.com/t/artie-labs/shared_invite/zt-1k28i8nja-W7G24qrRcJKeySDFLecFUg) 

<br/>

Depending on where you work, the latency in your data warehouse is often several hours to days old. This problem gets exacerbated as data volumes grow. <br/><br/>
Transfer reads from the change data capture (CDC) stream and provides an easy out of the box solution that only requires a simple configuration file and will replicate the data in your transactional database to your data warehouse. To do this, Transfer has the following features built-in:

- Automatic retries & idempotency. We take reliability seriously and it's feature 0. Latency reduction is nice, but doesn't matter if the data is wrong. We provide automatic retries and idempotency such that we will always achieve eventual consistency.
- Automatic table creation. Transfer will create the table in the designated database if it does not exist.
- Error reporting. Provide your Sentry API key and errors from data processing will appear in your Sentry project.
- Schema detection. Transfer will automatically detect column changes and apply them to the destination.
- Scalable architecture. Transfer's architecture stays the same whether we’re dealing with  1GB or 100+ TB of data.
- Sub-minute latency. Transfer is built with a consumer framework and is constantly streaming messages in the background. Say goodbye to schedulers!

Take a look at the [Running section](#running) on how you would be able to run Transfer for your workloads.

## Architecture
<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201719978-d9659515-6305-440f-b14a-f5d577a15457.png"/>
</div>

### Pre-requisites
As you can see from the architecture above, Transfer sits behind Kafka and expects CDC messages to be in a particular format. Please see the currently supported section on what sources and destinations are supported.

Kafka topic per table. The partition key must be the primary key for the row.
CDC connector (refer to supported section on supported connectors)
Supported OLTP & OLAP

If you are having trouble setting up CDC, please see the [examples folder](https://github.com/artie-labs/transfer/tree/master/examples) on how to configure a test database to emit CDC messages to Kafka.

## <a name="running"></a>Running

### Locally
There are multiple ways to get an image of Transfer:
1. Pull the image from [Dockerhub](https://hub.docker.com/r/artielabs/transfer)
1. Git clone the repo and run 
```sh
go build
```
3. Download via `go`
```sh
go get github.com/artie-labs/transfer
```

_Once you have the image, provide a configuration file and run `transfer --config config.yaml`_

_Optional Flags_
* You may also pass in `-v` and Transfer will emit debug level logs.


### Docker and examples

The Transfer base image is published on Docker Hub and can be viewed [here](https://hub.docker.com/r/artielabs/transfer). 

Take a look at the [examples folder](https://github.com/artie-labs/transfer/tree/master/examples) to see end-end examples on how Transfer works.

### Kubernetes
Simply define a K8 deployment, use Transfer as the base image and provide a configuration file. Then you will need to run 
```sh
./transfer --config path_to_config
```

See [here for an example](https://github.com/artie-labs/transfer/blob/master/examples/mongodb/Dockerfile) 

## What is currently supported?
Transfer is aiming to provide coverage across all OLTPs and OLAPs databases. Currently Transfer supports:

- OLAPs:
    - Snowflake
- OLTPs:
    - MongoDB (Debezium)
    - Postgres (Debezium w/ wal2json)

_If the database you are using is not on the list, feel free to file for a [feature request](https://github.com/artie-labs/transfer/issues/new)._

## Configuration File

Note: Keys here are formatted in dot notation for readability purposes, please ensure that the proper nesting is done when dumping this into config.yaml. Take a look at the [example config.yaml](https://github.com/artie-labs/transfer/blob/master/examples/mongodb/config.yaml) for additional reference. 

| Key| Type | Optional | Description |
| ------------ | --- | - | ---------------------|
| outputSource | String | N | This is the destination. <br/> Supported values are currently: `snowflake`, `test` |
| kafka | Object | N | This is the parent object, please see below |
| kafka.bootstrapServer | String | N | URL to the Kafka server, including the port number. Example: `localhost:9092` |
| kafka.groupID | String | N | Kafka consumer group ID |
| kafka.username | String | Y | Kafka username (Transfer currently only supports plain SASL or no auth) |
| kafka.password | String | Y | Kafka password |
| kafka.topicConfigs | Array | N | TopicConfigs is an array of TopicConfig objects, please see below on what each topicConfig object looks like. |
| kafka.topicConfigs[0].db | String | N | Name of the database in Snowflake |
| kafka.topicConfigs[0].tableName | String | N | Name of the table in Snowflake |
| kafka.topicConfigs[0].schema | String | N | Name of the schema in Snowflake |
| kafka.topicConfigs[0].topic | String | N | Name of the Kafka topic |
| kafka.topicConfigs[0].idempotentKey | String | Y | Name of the column that is used for idempotency. This field is highly recommended. <br/> For example: `updated_at` or another timestamp column. |
| kafka.topicConfigs[0].cdcFormat | String | N | Name of the CDC connector (thus format) we should be expecting to parse against. <br/> Currently, the supported values are: `debezium.postgres.wal2json`, `debezium.mongodb` |
| kafka.topicConfigs[0].cdcKeyFormat | String | Y | Format for what Kafka Connect will the key to be. This is called `key.converter` in the Kafka Connect properties file. <br/> The supported values are: `org.apache.kafka.connect.storage.StringConverter`, `org.apache.kafka.connect.json.JsonConverter` <br/> If not provided, the default value will be `org.apache.kafka.connect.storage.StringConverter`|
| snowflake | Object | N | This is the parent object, please see below |
| snowflake.account | String | N | Snowflake Account ID |
| snowflake.username | String | N | Snowflake username |
| snowflake.password | String | N | Snowflake password |
| snowflake.warehouse | String | N | Snowflake warehouse name |
| snowflake.region | String | N | Snowflake region |
| reporting.sentry.dsn | String| Y | DSN for Sentry alerts. If blank, will just go to standard out. |

## Limitations
**Postgres Debezium wal2json** <br/>
* `decimal.handling.mode` only works for `double` or `string`.<br/>
The default value is `precise` which will cast the value in `java.math.BigDecimal` and Transfer does not know how to decode that yet.
For further information on how to set this to be `string` or `double`, please [click here](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-debezium.html#connector-details)
* `key.converter.schemas.enable` and `value.converter.schemas.enable` both need to be set to `false`
* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`


**MongoDB Debezium** <br/>
* `key.converter.schemas.enable` and `value.converter.schemas.enable` both need to be set to `false`
* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`

## Tests
Transfer is written in Go and uses [counterfeiter](https://github.com/maxbrunsfeld/counterfeiter) to mock. 
To run the tests, run the following commands:

```sh
make generate
make test
```

## Release

```sh
docker build .
docker tag IMAGE_ID artielabs/transfer:0.1
docker push artielabs/transfer:0.1
```


## License

Transfer is licensed under ELv2. Please see the [LICENSE](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt) file for additional information. If you have any licensing questions please email hi@artie.so.
