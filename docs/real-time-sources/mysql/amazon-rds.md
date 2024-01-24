# Amazon RDS

## Creating a service account

```sql
CREATE USER 'artie_transfer'@'%' IDENTIFIED BY 'password';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO artie_transfer@'%';
```

## Enabling Binary Log Access

To enable binary logging, you'll need to update your RDS instance's parameter group.

> You can access this by going to RDS, Parameter groups, Create parameter group

<figure><img src="../../.gitbook/assets/image (1).png" alt=""><figcaption></figcaption></figure>

Go into the newly created parameter group and click edit and change `binlog_format` to `ROW`

<figure><img src="../../.gitbook/assets/image (2).png" alt=""><figcaption></figcaption></figure>
