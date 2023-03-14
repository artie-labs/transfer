# MySQL example

This example does the following:
1. Run Debezium server
2. Runs MySQL image
3. Runs Transfer which outputs the merge commands to stdout

# Running
```
docker-compose build
docker-compose up
```

# Connecting to MySQL
Once you have docker-compose running, the MySQL instance can be reached by the following command.

```bash
mysql -h 0.0.0.0 -u mysqluser -p
# Password is mysqlpw
```

```sql
UPDATE inventory.customers SET first_name = 'Artie' where id = 1001;
-- Do any DML and DDL you would like.
-- And it'll show up in the console :)
```
