---
description: >-
  In this page, we'll go over how to enable write-ahead log (WAL) for your AWS
  RDS instance running PostgreSQL.
---

# Amazon RDS - PostgreSQL

## Turning on logical replication

Go into the RDS dashboard and click create a parameter group.

Make sure you select the right DB family group. Use `postgres10` if your database is running on `10.X`.

<figure><img src="../../.gitbook/assets/image (19).png" alt=""><figcaption></figcaption></figure>

<figure><img src="../../.gitbook/assets/image (33).png" alt=""><figcaption></figcaption></figure>

Find the `rds.logical_replication` parameter and set it to `1`.

<figure><img src="../../.gitbook/assets/image (26).png" alt=""><figcaption></figcaption></figure>

Now, go to your database, modify the instance to attach the newly created parameter group and then restart your database.

{% hint style="info" %}
Note **`rds.logical_replication`**is a [static](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.Parameters.html) parameter, which requires DB reboot to have effect.
{% endhint %}

<figure><img src="../../.gitbook/assets/image (16).png" alt=""><figcaption></figcaption></figure>
