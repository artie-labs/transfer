---
description: >-
  In this section, we will go over how to gather your credentials and prepare
  your Microsoft SQL Server to start providing CDC logs.
---

# Microsoft SQL Server

### Settings required

| Name                        | Description                                                                                                                                             | Default value |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | :-----------: |
| Host Name                   | IP address or hostname of your database server.                                                                                                         |       -       |
| Port                        | Port for where your server is running.                                                                                                                  |      1433     |
| <p>Username<br>Password</p> | <p>Username and Password for authentication into your database.</p><p></p><p>See below if you'd like to create a service account specific to Artie.</p> |       -       |
| Database                    | The name of the database that you want to capture changes for.                                                                                          |       -       |

### Creating a service account

```sql
USE DATABASE_NAME;
CREATE LOGIN artie WITH PASSWORD = 'PASSWORD';
CREATE USER artie FOR LOGIN artie;
GRANT SELECT on DATABASE::DATABASE_NAME to artie;
```

### Enabling CDC

To enable CDC for SQL server, you'll need to **enable it at the database and table level**.

1. **Enable CDC for your database**

```sql
USE DATABASE_NAME;
EXEC sys.sp_cdc_enable_db;

-- Now specify the retention period of your CDC logs, retention is specified as mins.
-- We recommend you setting this between 24h to 7 days
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080; -- 7 days
```

2. **Enable CDC for your tables**

```sql
exec sys.sp_cdc_enable_table 
@source_schema = 'SCHEMA_NAME',
@source_name = 'TABLE_NAME',
-- You can specify the service_account as the @role_name to restrict access
@role_name = null;
```

### Troubleshooting

#### **Unique index override**

By default, `sys.sp_cdc_enable_table` will use the primary keys of the table as the unique identifiers. If you are running into issues with this, you can optionally set `@index_name` to an unique index of your choice.

For example, if you had a table that looked like this

```sql
CREATE TABLE orders (
    id INTEGER IDENTITY(1001,1) NOT NULL PRIMARY KEY,
    order_date DATE NOT NULL,
    purchaser INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    FOREIGN KEY (purchaser) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
```

If you don't want to use `id`, you can do something like this:

```sql
ALTER TABLE orders ADD COLUMN prefix VARCHAR(255) DEFAULT 'orders' NOT NULL;
-- Create unique index
CREATE UNIQUE INDEX cdc_index ON dbo.orders (id, prefix);
-- When enabling CDC, use cdc_index instead of the primary key
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo',@source_name = 'orders',
@role_name = NULL, @index_name= 'cdc_index';
```
