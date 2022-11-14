<div align="center">
  <img src="https://user-images.githubusercontent.com/4412200/201717557-17c79b66-2303-4141-bea2-87382fb02613.png" />
  <h3>Transfer by Artie</h3>
  <p>⚡️ Blazing fast data replication between OLTP and OLAP databases⚡️</p>
  <b><a href="https://artie.so">Learn more »</a></b>
</div>

[![Go tests](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml/badge.svg)](https://github.com/artie-labs/transfer/actions/workflows/gha-go-test.yml) [![ELv2](https://user-images.githubusercontent.com/4412200/201544613-a7197bc4-8b61-4fc5-bf09-68ee10133fd7.svg)](https://github.com/artie-labs/transfer/LICENSE.txt)

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

## Build



## Tests


## Disorganized atm

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
