---
description: >-
  In this page, we'll go over how to enable write-ahead log (WAL) for your GCP
  CloudSQL instance running PostgreSQL.
---

# GCP CloudSQL - PostgreSQL

## Turning on Logical Decoding Flag

We'll need this flag to be turned on so that we are able to use replication slots. To turn this on, click `Edit`, then `Flags` and enable `cloudsql.logical_decoding` to be ON.

<figure><img src="../../.gitbook/assets/image (29).png" alt=""><figcaption></figcaption></figure>

## Ensure the service account has sufficient permissions

Now that we have turned on `logical_decoding`, we need to make sure the service account has access to create a replication slot.

```sql
alter user postgres with replication;
```
