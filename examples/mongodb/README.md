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
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mongodb-connector.json

# Now initiate the replica set and insert some dummy data.
docker-compose -f docker-compose.yaml exec mongodb bash -c '/usr/local/bin/init-inventory.sh'

# Now, if you want to connect to the Mongo shell and insert more data, go right ahead
docker-compose -f docker-compose.yaml exec mongodb bash -c 'mongo -u $MONGODB_USER -p $MONGODB_PASSWORD --authenticationDatabase admin inventory'
db.customers.insert([
    { _id : NumberLong("1020"), first_name : 'Robin',
    last_name : 'Tang', email : 'robin@example.com', unique_id : UUID(),
    test_bool_false: false, test_bool_true: true, new_id: ObjectId(),
    test_decimal: NumberDecimal("13.37"), test_int: NumberInt("1337"),
    test_decimal_2: 13.37, test_list: [1, 2, 3, 4, "hello"], test_null: null, test_ts: Timestamp(42, 1), test_nested_object: {a: { b: { c: "hello"}}}}
]);
```
