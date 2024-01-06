---
description: >-
  BigQuery Time Partitioning is a good way to reduce the number of bytes
  processed which lowers your bill and increases Artie Transfer's performance!
---

# Enabling BigQuery Time Partitioning

### What is a partitioned table?

A partitioned table is divided into segments, called partitions, that make it easier to manage and query your data. By dividing a large table into smaller partitions, you can improve query performance and control costs by reducing the number of bytes read by a query. You partition tables by specifying a partition column which is used to segment the table.

### Why should we use table partitions?

* Improve query performance by scanning a partition.
* When you exceed the [standard table quota](https://cloud.google.com/bigquery/quotas#standard\_tables).
* Gain partition-level management features such as writing to or deleting partition(s) within a table.
* Reduce the number of bytes processed + reduce your BigQuery bill

{% hint style="info" %}
Note: A BigQuery table **can have up to 4000 partitions**. So, if you picked the daily granularity, you have enough partitions for up to 10.9 years worth of unique partitions!
{% endhint %}

### What are the different kinds of [partitioning strategies](https://cloud.google.com/bigquery/docs/partitioned-tables#types\_of\_partitioning)?

| Partitioning type                                                                                                      | Description                                                                                                                                                                                                                                                     | Example                                                                                                                                                       |
| ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Time partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#date\_timestamp\_partitioned\_tables)    | <p>Partitioning a particular column that is a TIMESTAMP. <br><br>BigQuery <a href="https://cloud.google.com/bigquery/docs/partitioned-tables#partition_decorators">allows</a> hourly, daily, monthly, yearly and integer range partitioning intervals. </p>     | <p>Column: timestamp<br>Partitioning granularity: daily<br></p>                                                                                               |
| [Integer range or interval based partitions](https://cloud.google.com/bigquery/docs/partitioned-tables#integer\_range) | Partitioning off of a range of values for a given column.                                                                                                                                                                                                       | <p>Say you have a column called customer_id and there are 100 values.<br><br>You can specify to have values 0-9 go to one partition, 10-19 the next, etc.</p> |
| [Ingestion-based](https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion\_time)                           | <p>This is when the row was inserted.<br><br>This is <strong>not recommended</strong>, because it requires storing additional metadata to know when this row was inserted. If we don't specify this upon a merge, we will end up creating duplicate copies.</p> | NA                                                                                                                                                            |

## Turning on BigQuery Partitions

The steps are as follows:

1. Pause Artie Transfer deployment (this will only pause Artie Transfer, Debezium will still be running and capturing the changes).
2. Create the table with the right partitioning strategy (e.g. `leads_copy`)
3. Copy the main table into the new partitioned table&#x20;
4. Drop the old table (`leads`) and [rename](https://cloud.google.com/bigquery/docs/managing-tables#renaming-table) the new partitioned table. (`leads_copy`)
5. Within the Artie deployment page, update the table settings
6. Resume Artie Transfer

### Pausing and resuming Artie Transfer

<figure><img src="../.gitbook/assets/image (6).png" alt=""><figcaption></figcaption></figure>

### Editing Artie Transfer table settings

<figure><img src="../.gitbook/assets/Jul-28-2023 21-41-50.gif" alt=""><figcaption></figcaption></figure>
