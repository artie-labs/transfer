---
description: We will go over how we can add primary key(s) to tables that do not have them.
---

# Tables without primary key(s)

## Why do we only replicate tables with primary key(s)?

1. We need a way to uniquely identify each row as we use the primary key(s) as the partition key in Kafka to **guarantee ordering**.&#x20;
2. For us to guarantee data integrity, we perform `MERGE` with the table's primary key(s)

## Scenario #1: I already have pseudo primary key(s)

Let's use the example of `users_no_pk` as our example.

```sql
CREATE TABLE users_no_pk (
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL
);
```

Email is unique, however not the primary key. To make `email` unique, we'll run the following command:

```sql
ALTER TABLE users_no_pk ADD PRIMARY KEY (email);
```

## Scenario #2: How do I add primary key(s) to an existing table?

Let's use this table `no_primary_keys` as our example.

```sql
CREATE TABLE no_primary_keys (
	key VARCHAR(5),
	value bool
);

INSERT INTO no_primary_keys (key, value) VALUES ('foo', true), ('bar', false);
```

```bash
postgres=# select * from no_primary_keys;
 key | value
-----+------
 bar | f    
 foo | f  
(2 rows)
```

To add a primary key, we will now issue this DDL query:

```sql
ALTER TABLE no_primary_keys ADD COLUMN pk SERIAL PRIMARY KEY;
-- This will automatically backfill the existing rows and start a sequence
-- Such that subsequent rows will automatically have a primary key value.
```

```sql
postgres=# select * from no_primary_keys;
 key | value | pk
-----+-------+----
 bar | f     |  2
 foo | f     |  1
(2 rows)
```

```sql
-- No code changes required on your application side as you can
-- continue inserting data w/o referencing primary keys
INSERT INTO no_primary_keys (key, value) VALUES ('qux', false);
```

```bash
postgres=# select * from no_primary_keys;
 key | value | pk
-----+-------+----
 bar | f     |  2
 foo | f     |  1
 qux | f     |  3
(3 rows)
```
