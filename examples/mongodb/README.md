# MongoDB Example


```sh
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mongodb-connector.json


docker-compose -f docker-compose.yaml exec mongodb bash -c '/usr/local/bin/init-inventory.sh'


docker-compose -f docker-compose.yaml exec mongodb bash -c 'mongo -u $MONGODB_USER -p $MONGODB_PASSWORD --authenticationDatabase admin inventory'




db.customers.insert([
    { _id : NumberLong("1020"), first_name : 'Robin', 
    last_name : 'Tang', email : 'thebob@example.com', unique_id : UUID(), 
    test_bool_false: false, test_bool_true: true, new_id: ObjectId(),
    test_decimal: NumberDecimal("13.37"), test_int: NumberInt("1337"),
    test_decimal_2: 13.37, test_list: [1, 2, 3, 4, "hello"], test_null: null, test_ts: Timestamp(42, 1), test_nested_object: {a: { b: { c: "hello"}}}}
]);

```