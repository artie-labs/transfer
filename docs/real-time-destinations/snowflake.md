---
description: Going over how to find your Snowflake settings and outline the typing support.
---

# Snowflake

## Finding your Snowflake settings

### Getting your username and password

<figure><img src="../.gitbook/assets/image (1) (2).png" alt=""><figcaption><p>Create a service account for Transfer to use</p></figcaption></figure>

### Getting the Snowflake account identifier

In order for workloads to uniquely identify accounts, we need to pass in your [account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).&#x20;

### Which data warehouse to use?

Part of Snowflake's pricing model is virtual warehouse usage and price scales based on the size of the virtual warehouse.&#x20;

{% hint style="info" %}
Consider creating a new virtual data warehouse for Transfer so that you can size it independently of your other DWH workloads. Keep in mind that data warehouse size will determine performance and throughput! \
\
For the most optimal usage, consider leveraging warehouse suspension policies and size your warehouse appropriately depending on your workloads. Click [here to learn more](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#warehouse-size) from Snowflake's website.
{% endhint %}

<figure><img src="../.gitbook/assets/image (3) (1).png" alt=""><figcaption></figcaption></figure>



## Typing

| Snowflake type                                                | Artie type                  |
| ------------------------------------------------------------- | --------------------------- |
| Number / Decimal / Numeric / Double / Double Precision / Real | Float                       |
| Int / Integer / Big Int / Small Int / Tiny Int Byte Int       | Integer                     |
| Float / Float4 / Float8                                       | Float                       |
| Varchar                                                       | String                      |
| Char, Character                                               | String                      |
| String                                                        | String                      |
| Text                                                          | String                      |
| Binary / Var Binary                                           | 🟠 Currently not supported. |
| Boolean                                                       | Boolean                     |
| Date                                                          | Date                        |
| Datetime                                                      | Datetime                    |
| Timestamp                                                     | Datetime                    |
| Timestamp LTZ                                                 | Datetime                    |
| Timestamp NTZ                                                 | Datetime with UTC TZ        |
| Timestamp TZ                                                  | Datetime                    |
| Variant                                                       | Struct                      |
| Object                                                        | Struct                      |
| Array                                                         | Array                       |
| Geography                                                     | 🟠 Currently not supported. |
| Geometry                                                      | 🟠 Currently not supported. |

