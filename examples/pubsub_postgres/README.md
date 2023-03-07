# Postgres Example

In this example, we will running Debezium server to retrieve PostgreSQL logs, publish them to Pub/Sub and Transfer will then consume these logs and write it to a fake database.


## Pre-requisites

A Google Pub/Sub account that has editor access. TODO - Link to Terraform code in this folder.

## Running

To run this, you'll need to install Docker. We will be running 3 images.

1. Debezium Server
2. Postgres
3. Transfer

_Note: Snowflake does not have a development Docker image, so the Mock DB will just output the function calls_

### Initial set up
```sh
docker-compose build

docker-compose up

```
