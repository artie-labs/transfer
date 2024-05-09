---
description: Going over how to find your Snowflake settings and outline the typing support.
---

# Snowflake

## Finding your Snowflake settings

### Getting your username and password

<figure><img src="../.gitbook/assets/image (10).png" alt=""><figcaption><p>Create a service account for Transfer to use</p></figcaption></figure>

### Getting the Snowflake account identifier

In order for workloads to uniquely identify accounts, we need to pass in your [account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).&#x20;

<figure><img src="../.gitbook/assets/snowflake_account_url.gif" alt=""><figcaption><p>Retrieving Account URL</p></figcaption></figure>

### Which data warehouse to use?

Part of Snowflake's pricing model is virtual warehouse usage and price scales based on the size of the virtual warehouse.&#x20;

{% hint style="info" %}
Consider creating a new virtual data warehouse for Transfer so that you can size it independently of your other DWH workloads. Keep in mind that data warehouse size will determine performance and throughput! \
\
For the most optimal usage, consider leveraging warehouse suspension policies and size your warehouse appropriately depending on your workloads. Click [here to learn more](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#warehouse-size) from Snowflake's website.
{% endhint %}

<figure><img src="../.gitbook/assets/image (27).png" alt=""><figcaption></figcaption></figure>

## Creating a service account for Artie

Paste and modify the variables to create an account for Artie.

```sql
BEGIN TRANSACTION;
    USE ROLE ACCOUNTADMIN; -- This combines both SYSADMIN and SECURITYADMIN

    -- IMPORTANT, PLEASE FILL THIS OUT AND SAVE THIS --
    SET ROLE_NAME = 'ARTIE_TRANSFER_ROLE';
    SET SERVICE_USER = 'ARTIE';
    SET SERVICE_PW = 'PASSWORD';
    -- NOTE: If you already have a DWH, you can use that, or create a separate one for Artie
    -- If your default DWH is a larger size, you may consider creating a dedicated one for Artie that's a smaller size
    -- To optimize your spend. See https://docs.artie.com/configurations/real-time-destinations/snowflake#which-data-warehouse-to-use for more details.
    SET DWH_NAME = UPPER('DWH');
    SET DB_NAME = UPPER('DB_NAME');
    SET SCHEMA_NAME = UPPER('public');
    -- END IMPORTANT --
    SET DB_SCHEMA_NAME = CONCAT($DB_NAME, '.', $SCHEMA_NAME);

    CREATE ROLE IF NOT EXISTS identifier($ROLE_NAME);
    CREATE USER IF NOT EXISTS identifier($SERVICE_USER)
        password = $SERVICE_PW
        default_role = $ROLE_NAME;
    GRANT ROLE identifier($role_name) to USER identifier($SERVICE_USER);
    CREATE WAREHOUSE IF NOT EXISTS identifier($DWH_NAME)
        warehouse_size = xsmall
        warehouse_type = standard
        auto_suspend = 10
        auto_resume = true
        initially_suspended = true;

    GRANT USAGE ON WAREHOUSE identifier($DWH_NAME) TO ROLE identifier($ROLE_NAME);
    GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_NAME);
    GRANT ALL PRIVILEGES ON SCHEMA identifier($DB_SCHEMA_NAME) TO ROLE IDENTIFIER($ROLE_NAME);

    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA identifier($DB_SCHEMA_NAME) TO ROLE IDENTIFIER($ROLE_NAME);
    GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA identifier($DB_SCHEMA_NAME) TO ROLE IDENTIFIER($ROLE_NAME);
COMMIT;

```

## Troubleshooting

### Why am I not able to query or operate table?

Snowflake's native RBAC makes it so that the account that created the resource is the native owner. To change this, assign the ARTIE service account's role to your account and you will be able to operate on the table. See the GIF below on how to fix this problem! \[[source](https://community.snowflake.com/s/question/0D50Z00008GUDFlSAP/insufficient-privileges-to-operate-on-table-even-running-as-accountadmin)]

<figure><img src="../.gitbook/assets/SFLK.gif" alt=""><figcaption></figcaption></figure>

