<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201717557-17c79b66-2303-4141-bea2-87382fb02613.png" />
  <h3>Artie Transfer</h3>
  <p>⚡️ Blazing fast data replication between OLTP and OLAP databases ⚡️</p>
  <b><a target="_blank" href="https://artie.so" >Learn more »</a></b>
</div>
<br/>

[![Go tests](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg)](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml) [![ELv2](https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg)](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt) [<img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack">](https://artie.so/slack) 

<br/>

Depending on where you work, the latency in your data warehouse is often several hours to days old. This problem gets exacerbated as data volumes grow. <br/><br/>
Artie Transfer reads from the change data capture (CDC) stream and provides an easy out of the box solution that only requires a simple configuration file and will replicate the data in your transactional database to your data warehouse. To do this, Transfer has the following features built-in:

- Automatic retries & idempotency. We take reliability seriously and it's feature 0. Latency reduction is nice, but doesn't matter if the data is wrong. We provide automatic retries and idempotency such that we will always achieve eventual consistency.
- Automatic table creation. Transfer will create the table in the designated database if the table doesn't exist.
- Error reporting. Provide your Sentry API key and errors from data processing will appear in your Sentry project.
- Schema detection. Transfer will automatically detect column changes and apply them to the destination.
- Scalable architecture. Transfer's architecture stays the same whether we’re dealing with  1GB or 100+ TB of data.
- Sub-minute latency. Transfer is built with a consumer framework and is constantly streaming messages in the background. Say goodbye to schedulers!

Take a look at the [Getting started](#getting-started) on how to get started with Artie Transfer!

## Architecture
<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201719978-d9659515-6305-440f-b14a-f5d577a15457.png"/>
</div>

### Pre-requisites
As you can see from the architecture above, Transfer sits behind Kafka and expects CDC messages to be in a particular format. Please see the currently supported section on what sources and destinations are supported.

The optimal set-up looks something like this:
* Kafka topic per table (so we can toggle number of partitions based on throughput)
* Partition key is the primary key for the table (so we avoid out-of-order writes at the row level)

To see the current supported databases, check out the [Supported section](#what-is-currently-supported)

## Examples

To run Artie Transfer's stack locally, please refer to the [examples folder](https://github.com/artie-labs/transfer/tree/master/examples).

## <a name="getting-started"></a>Getting started

[Getting started guide](https://docs.artie.so/configurations/running-transfer/overview)

## What is currently supported?
Transfer is aiming to provide coverage across all OLTPs and OLAPs databases. Currently Transfer supports:

- Message Queues
  - Kafka (default)
  - Google Pub/Sub

- [Destinations](https://docs.artie.so/configurations/real-time-destinations/overview):
    - Snowflake
    - BigQuery

- [Sources](https://docs.artie.so/configurations/real-time-sources/overview):
    - MongoDB (w/ Debezium)
    - Postgres (w/ Debezium), we support the following replication slot plug-ins: `pgoutput, decoderbufs, wal2json`

_If the database you are using is not on the list, feel free to file for a [feature request](https://github.com/artie-labs/transfer/issues/new)._

## Configuration File
* [Artie Transfer configuration file guide](https://docs.artie.so/configurations/running-transfer/options)
* [Examples of configuration files](https://docs.artie.so/configurations/running-transfer/examples)


## Limitations
_Note: If any of these limitations are blocking you from using Transfer. Feel free to contribute or file a bug and we'll get this prioritized!</br>
The long term goal for Artie Transfer is to be able to extend the service to have as little of these limitations as possible._

* [PostgreSQL](https://docs.artie.so/configurations/real-time-sources/postgresql#things-to-note-if-you-are-running-your-own-debezium)
* [MongoDB](https://docs.artie.so/configurations/real-time-sources/mongodb#things-to-note-if-you-are-running-your-own-debezium)

## Telemetry

[Artie Transfer's telemetry guide](https://docs.artie.so/configurations/telemetry/overview)

## Tests
Transfer is written in Go and uses [counterfeiter](https://github.com/maxbrunsfeld/counterfeiter) to mock. 
To run the tests, run the following commands:

```sh
make generate
make test
```

## Release

Artie Transfer is released through [GoReleaser](https://goreleaser.com/), and we use it to cross-compile our binaries on the [releases](https://github.com/artie-labs/transfer/releases) as well as our Dockerhub. If your operating system or architecture is not supported, please file a feature request!

## License

Artie Transfer is licensed under ELv2. Please see the [LICENSE](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt) file for additional information. If you have any licensing questions please email hi@artie.so.
