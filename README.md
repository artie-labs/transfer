<div align="center">
  <img height="150px" src="https://github.com/artie-labs/transfer/assets/4412200/238df0c7-6087-4ddc-b83b-24638212af6a"/>
  <h3>Artie Transfer</h3>
  <p><b>⚡️ Blazing fast data replication between OLTP and OLAP databases ⚡️</b></p>
  <a href="https://artie.so/slack"><img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack"/></a>
  <a href="https://docs.artie.so/running-transfer/overview"><img src="https://user-images.githubusercontent.com/4412200/226736695-6b8b9abd-c227-41c7-89a1-805a04c90d08.png"/></a>
  <a href="https://github.com/artie-labs/transfer/blob/master/LICENSE.txt"><img src="https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg"/></a>
  <img src="https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg"/>
  <br/>
  <b><a target="_blank" href="https://artie.so" >Learn more »</a></b>
</div>
<br/>

Artie Transfer is a real time data replication solution for databases and data warehouses/data lakes.

Typical ETL solutions leverage batched processes or schedulers (DAGs, Airflow), which means the data latency in the downstream data warehouse is often several hours to days old. This problem gets exacerbated as data volumes grow (batched processes take increasingly longer to run). 

Artie leverages change data capture (CDC) and stream processing to perform data syncs in a more efficient way, which enables sub-minute latency. Use Artie Transfer to reduce data latency from several hours to seconds! 

Benefits of Artie Transfer:

- Sub-minute data latency so you always have access to live production data.
- Easy to use: just set up a simple configuration file, and you're good to go!
- Automatic table creation and schema detection.
- Artie has automatic retries and its processing is idempotent.
- Built to scale: handle anywhere from 1GB to 100+ TB of data.
- Built-in error reporting along with rich telemetry statistics.

Take a look at the [Getting started](#getting-started) on how to get started with Artie Transfer!

## Architecture
<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201719978-d9659515-6305-440f-b14a-f5d577a15457.png"/>
</div>

### Pre-requisites
As you can see from the architecture above, Artie Transfer sits behind Kafka and expects CDC messages to be in a particular format. Please see the currently supported section on what sources and destinations are supported.

The optimal set-up looks something like this:
* One Kafka topic per table (such that we can toggle the number of partitions based on throughput)
* The partition key is the primary key for the table (to avoid out-of-order writes at the row level)

To see all of the supported databases, check out the [Supported section](#what-is-currently-supported)

## Examples

To run Artie Transfer's stack locally, please refer to the [examples folder](https://github.com/artie-labs/transfer/tree/master/examples).

## <a name="getting-started"></a>Getting started

[Getting started guide](https://docs.artie.so/running-transfer/overview)

## What is currently supported?
Transfer is aiming to provide coverage across all OLTPs and OLAPs databases. Currently Transfer supports:

- Message Queues
  - Kafka (default)
  - Google Pub/Sub

- [Destinations](https://docs.artie.so/real-time-destinations/overview):
    - Snowflake
    - BigQuery
    - Redshift
    - S3

- [Sources](https://docs.artie.so/real-time-sources/overview):
    - MongoDB
    - PostgreSQL, we support the following replication slot plug-ins: `pgoutput, decoderbufs, wal2json`
    - MySQL

_If the database you are using is not on the list, feel free to file for a [feature request](https://github.com/artie-labs/transfer/issues/new)._

## Configuration File
* [Artie Transfer configuration file guide](https://docs.artie.so/running-transfer/options)
* [Examples of configuration files](https://docs.artie.so/running-transfer/examples)

## Telemetry

[Artie Transfer's telemetry guide](https://docs.artie.so/telemetry/overview)

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
