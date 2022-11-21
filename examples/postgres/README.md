# MongoDB Example

## Running

To run this, you'll need to install Docker. We will be running 5 images.

1. zookeeper
2. kafka
3. MongoDB
4. Debezium (pulling the data from Mongo and publishing to Kafka)
5. Transfer (pulling Kafka and writing against a test DB)

_Note: Snowflake does not have a development Docker image, so the Mock DB will just output the function calls_

### Initial set up
```sh
docker-compose build

docker-compose up

```

### Registering the connector
```sh
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres-connector.json

## Play around within the Postgres server (insert, update, delete) will now all work.
docker-compose -f docker-compose.yaml exec postgres env PGOPTIONS="--search_path=inventory" bash -c 'psql -U $POSTGRES_USER postgres'

```
