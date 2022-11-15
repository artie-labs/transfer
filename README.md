<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201717557-17c79b66-2303-4141-bea2-87382fb02613.png" />
  <h3>Transfer by Artie</h3>
  <p>⚡️ Blazing fast data replication between OLTP and OLAP databases ⚡️</p>
  <b><a target="_blank" href="https://artie.so" >Learn more »</a></b>
</div>

[![Go tests](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg)](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml) [![ELv2](https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg)](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt)

<br/>

Depending on where you work, the latency within your data warehouse is often several hours to days old. This problem gets exacerbated as data volumes grow. Transfer reads from the change data capture (CDC) stream and provides an easy out of the box solution that only requires a simple configuration file and supports the following features:

- Automatic retries & idempotency. We take reliability seriously and it's feature 0. Latency reduction is nice, but doesn't matter if the data is wrong. We provide automatic retries and idempotency such that we will always achieve eventual consistency.
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
As you can see from the architecture above , Transfer sits behind Kafka and expects CDC messages to be in a particular format. Please see the currently supported section on what sources and destinations are supported.

Kafka topic per table. Partition key must be the primary key for the row.
CDC connector (refer to supported section on supported connectors)
Supported OLTP & OLAP

If you are having trouble setting up CDC, please see the examples folder on how to configure a test database to emit CDC messages to Kafka.

## <a name="running"></a>Running

### Locally
There are multiple ways to get an image of Transfer:
1. Pull the image from [Dockerhub](https://hub.docker.com/r/artielabs/transfer)
1. Git clone the repo and run `go build`
1. `go get github.com/artie-labs/transfer`

_Once you have the image, provide a configuration file and run `transfer --config config.yaml`_

### Docker / Kubernetes

Simply define a Kubernetes deployment, pull the [Docker image](https://hub.docker.com/r/artielabs/transfer) and provide a configuration file. See the examples folder for k8 references. See this [Dockerfile](https://github.com/artie-labs/transfer/tree/master/docker_postres) under `/examples` for a sample Dockerfile, you'd simply need to redefine your `config.yaml` and away you go.

## What is currently supported?
Transfer is aimed to provide coverage across all OTLPs and OLAPs. Currently, Transfer provides:

- OLAPs:
    - Snowflake
- OTLPs:
    - MongoDB (Debezium with CES)
    - Postgres (Debezium w/ wal2json)

_If the database you are using is not on the list, feel free to file for a [feature request](https://github.com/artie-labs/transfer/issues/new)._

## Configuration File

Note: Keys here are formatted in dot notation for readability purposes, please ensure that the proper nesting is done when dumping this into config.yaml. Take a look at the [example config.yaml](https://github.com/artie-labs/transfer/blob/master/examples/postgres_config.yaml) for additional reference. 


For example a.b: foo` should be rewritten as
```yaml
# Wrong
a.b: foo

# Correct
a:
   b: foo
```

| Key| Type | Optional | Description |
| ------------ | --- | - | ---------------------|
| output_source | String | N | This is the destination. <br/> Supported values are currently: `snowflake` |
| kafka | Object | N | This is the parent object, please see below |
| kafka.bootstrapServer | String | N | URL to the Kafka server, including the port number. Example: `localhost:9092` |
| kafka.groupID | String | N | Kafka consumer group ID |
| kafka.username | String | N | Kafka username (we currently only support user/password auth) |
| kafka.password | String | N | Kafka password |
| kafka.topicConfigs | Array | N | TopicConfigs is an array of TopicConfig objects, please see below on what each topicConfig object looks like. |
| kafka.topicConfigs[0].db | String | N | Name of the database in Snowflake |
| kafka.topicConfigs[0].tableName | String | N | Name of the table in Snowflake |
| kafka.topicConfigs[0].schema | String | N | Name of the schema in Snowflake |
| kafka.topicConfigs[0].topic | String | N | Name of the Kafka topic |
| kafka.topicConfigs[0].idempotentKey | String | N | Name of the column that is used for idempotency. <br/> For example: `updated_at` or another timestamp column. |
| kafka.topicConfigs[0].cdc_format | String | N | Name of the CDC connector (thus format) we should be expecting to parse against. <br/> Currently, the supported values are: `debezium.postgres.wal2json` |
| snowflake | Object | N | This is the parent object, please see below |
| snowflake.account | String | N | Snowflake Account ID |
| snowflake.username | String | N | Snowflake username |
| snowflake.password | String | N | Snowflake password |
| snowflake.warehouse | String | N | Snowflake warehouse name |
| snowflake.region | String | N | Snowflake region |
| reporting.sentry.dsn | String| Y | DSN for Sentry alerts. If blank, will just go to standard out. |

## Tests
Transfer is written in Go and uses [counterfeiter](https://github.com/maxbrunsfeld/counterfeiter) to mock. 
To run the tests, run the following commands:

```
make generate
make test
```

## License

Transfer is licensed under ELv2. Please see the [LICENSE](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt) file for additional information. If you have any licensing questions please email hi@artie.so.









## Installing pre-reqs
```bash
# Installs
brew install direnv
echo 'eval "$(direnv hook bash)"' >> ~/.bash_profile

brew install postgresql
brew install zookeeper
brew install kafka

# Starting svcs
brew services restart postgresql@14
brew services restart zookeeper
brew services restart kafka
```
