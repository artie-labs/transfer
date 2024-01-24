# Amazon RDS or Aurora

## Creating a service account

```sql
CREATE USER 'artie_transfer' IDENTIFIED BY 'password';
GRANT SELECT, RELOAD, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'artie_transfer';
```

## Enabling Binary Log Access

To enable binary logging, you'll need to update your RDS instance's parameter group.&#x20;

{% hint style="info" %}
To create a new parameter group, go to RDS, Parameter groups and click Create.

For Aurora clusters, please ensure that you are creating this parameter group as **DB cluster parameter group**.
{% endhint %}

<figure><img src="../../.gitbook/assets/Screenshot 2024-01-23 at 1.50.51 PM.png" alt=""><figcaption></figcaption></figure>

Once this has been created, click on the paramter group and find `binlog_format` and change it to `ROW`.&#x20;

<figure><img src="../../.gitbook/assets/image (2).png" alt=""><figcaption></figcaption></figure>

Now, go to your database and modify the instance to attach the newly created parameter group and restart your database so it can pick up the change.&#x20;

### Setting a retention period for binlog

If this value is unset (which will use engine's defaults) or is set too low, Artie may not have an opportunity to capture the changes before it gets purged. It's recommended that you set this value to between 24 hours to 7 days to minimize disruption.

```sql
-- Setting binlog retention to 7 days
CALL mysql.rds_set_configuration('binlog retention hours', 168);
```

### Troubleshooting

```sql
-- Check if binlogs are enabled
SHOW VARIABLES LIKE 'log_bin';

-- Show the actual binlogs
SHOW MASTER STATUS;

-- Show binlog settings
CALL mysql.rds_show_configuration;
```

