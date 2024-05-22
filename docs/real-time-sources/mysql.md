---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your MySQL to start providing CDC logs.
---

# MySQL

## Settings required

| Name               | Description                                                                                                                                    | Default value |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| Database Host Name | <p>IP address or hostname of your database server.<br><br>Make sure this instance is the <strong>primary instance or writer node</strong>.</p> | No default    |
| Database Port      | Port for where your server is running.                                                                                                         | `3306`        |
| Database Username  | Username for authentication into your database.                                                                                                | No default    |
| Database Password  | Password for authentication into your database.                                                                                                | No default    |
| Database Name      | The name of the database that you want to capture changes for.                                                                                 | No default.   |

## Additional

* [amazon-rds-or-aurora.md](mysql/amazon-rds-or-aurora.md "mention")

## Running it yourself

* `value.converter` must be set to `org.apache.kafka.connect.json.JsonConverter`
* `value.converter.schemas.enable` must be set to `true`
* [Example Debezium connector settings](https://github.com/artie-labs/transfer/blob/master/examples/postgres/register-postgres-connector.json)
