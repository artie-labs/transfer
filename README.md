<h1
 align="center">
 <img
      align="center"
      alt="Artie Transfer"
      src="https://github.com/user-attachments/assets/9aa54907-af10-433b-8659-c868d4479f79"
      style="width:100%;"
    />
</h1>
<div align="center">
  <h3>Artie Transfer</h3>
  <p>⚡️ Blazing fast data replication between OLTP and OLAP databases ⚡️</p>
  <a href="https://artie.com/slack"><img src="https://img.shields.io/badge/slack-@artie-blue.svg?logo=slack"/></a>
  <a href="https://artie.com/docs/open-source/running-artie/overview"><img src="https://user-images.githubusercontent.com/4412200/226736695-6b8b9abd-c227-41c7-89a1-805a04c90d08.png"/></a>
  <a href="https://github.com/artie-labs/transfer/blob/master/LICENSE.txt"><img src="https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg"/></a>
  <img src="https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg"/>
  <br/>
  <b><a target="_blank" href="https://artie.com" >Learn more »</a></b>
</div>
<br/>

Artie Transfer is a real-time data replication solution for databases and data warehouses/lakes.

Typical ETL solutions rely on batched processes or schedulers (i.e. DAGs, Airflow), which means the data in the downstream data warehouse is often several hours to days old. This problem is exacerbated as data volumes grow, as batched processes take increasingly longer to run.

Artie leverages change data capture (CDC) and stream processing to perform data syncs in a more efficient way, which enables sub-minute latency.

Benefits of Artie Transfer:

- Sub-minute data latency: always have access to live production data.
- Ease of use: just set up a simple configuration file, and you're good to go!
- Automatic table creation and schema detection: Artie infers schemas and automatically merges changes to downstream destinations.
- Reliability: Artie has automatic retries and processing is idempotent.
- Scalability: handle anywhere from 1GB to 100+ TB of data.
- Monitoring: built-in error reporting along with rich telemetry statistics.

Take a look at this [guide](#getting-started) to get started!

## Architecture

<div align="center">
  <img src="https://github.com/artie-labs/transfer/assets/4412200/a30a2ee1-7bdd-437c-9acb-ce6591654d18"/>
</div>

## Examples

To run Artie Transfer's stack locally, please refer to the [examples folder](https://github.com/artie-labs/transfer/tree/master/examples).

## Getting started

[Getting started guide](https://artie.com/docs/open-source/running-artie/overview)

## What is currently supported?

Transfer is aiming to provide coverage across all OLTPs and OLAPs databases. Currently Transfer supports:

- Message Queues
  - Kafka (default)

- [Destinations](https://artie.com/docs/destinations):
    - BigQuery
    - Databricks
    - Iceberg (through S3 Tables)
    - Microsoft SQL Server
    - Redshift
    - S3
    - Snowflake
    - PostgreSQL
    - MotherDuck

- [Sources](https://artie.com/docs/sources):
    - DocumentDB
    - DynamoDB
    - Microsoft SQL Server
    - MongoDB
    - MySQL
    - Oracle
    - PostgreSQL


_If the database you are using is not on the list, feel free to file for a [feature request](https://github.com/artie-labs/transfer/issues/new)._

## Configuration File

* [Artie Transfer configuration file guide](https://artie.com/docs/open-source/running-artie/options)
* [Examples of configuration files](https://artie.com/docs/open-source/running-artie/examples)

## Telemetry

[Artie Transfer's telemetry guide](https://www.artie.com/docs/monitoring/available-metrics)

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

Artie Transfer is licensed under ELv2. Please see the [LICENSE](https://github.com/artie-labs/transfer/blob/master/LICENSE.txt) file for additional information. If you have any licensing questions please email hi@artie.com.
