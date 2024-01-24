# Troubleshooting

## Performing major upgrades

To do this, you'll need to do the following:

1. Pause your Artie Deployment
2. Delete the replication slot
3. Perform the upgrade
4. Resume Artie Deployment

### Step #1 - Pausing Artie Deployment

You can do this by going to the Deployment overview and changing the status.

<figure><img src="../../.gitbook/assets/image (3).png" alt="" width="563"><figcaption><p>Changing Deployment status</p></figcaption></figure>

### Step #2 - Delete the replication slot

```sql
-- Find all the replication slots
SELECT * FROM pg_replication_slots;
-- Drop a particular replication slot by name
SELECT pg_drop_replication_slot('NAME');
```

