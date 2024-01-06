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

Instead of giving an admin user, you could opt to give a limited user with less permissions. When doing so, ensure that we have the ability to:

* Create (creating new tables)
* Delete (deleting staging tables)
* Alter (adding and dropping columns)

### Typing

| Redshift type     | Artie type |
| ----------------- | ---------- |
| BIGINT / INT8     | Integer    |
| Decimal / Numeric | Numeric    |
| Double Precision  | Float      |
| Boolean           | Boolean    |
| VARCHAR           | String     |
| Date              | Date       |
| Timestamp         | Timestamp  |
| Time              | Time       |
