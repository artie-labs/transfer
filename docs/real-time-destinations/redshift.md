---
description: Going over how to find your Redshift settings and outline the typing support.
---

# Redshift

## Finding your Redshift settings

To integrate Redshift with Artie, we will need the following settings.

1. Endpoint URL (which contains the URL, Port and Namespace).
2. Username and password.&#x20;
3. Ensure it's publicly accessible to Artie.

<figure><img src="../.gitbook/assets/image (20).png" alt=""><figcaption></figcaption></figure>

### Giving a limited user account

Instead of giving an admin user, you could opt to give a limited user with less permissions.&#x20;

```sql
CREATE USER artie_transfer WITH PASSWORD 'password';
GRANT SELECT, INSERT, UPDATE, DELETE, DROP, ALTER ON ALL TABLES IN SCHEMA schema_name TO artie_transfer;
```
