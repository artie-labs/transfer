---
description: Going over how to find your BigQuery settings and outline the typing support.
---

# BigQuery

## Finding your BigQuery settings

### Getting your project identifier

When you are in your GCP project, you can see your GCP project ID at the top of the navigation bar.

<figure><img src="../.gitbook/assets/image.png" alt=""><figcaption></figcaption></figure>

### Getting your default dataset

In order for Transfer to connect to BigQuery, we will need to specify a default dataset.&#x20;

{% hint style="info" %}
The default dataset is only meant to establish the initial connection with BigQuery. The actual dataset specified within `topicConfig` can be different from the default dataset!
{% endhint %}

<figure><img src="../.gitbook/assets/image (4).png" alt=""><figcaption></figcaption></figure>

### Retrieving your credentials

For best practice, create a separate service account for `Transfer` and download the keys by following [this guide](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) from Google!

## Typing

| BigQuery type           | Artie type                                                                                                                                                                                                                                                            |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Array                   | <p>Array&#x3C;String>.<br><br>BigQuery <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type">requires elements</a> within an array to be typed to avoid nested arrays. As such, Artie standardized on Array&#x3C;String>.</p> |
| String                  | String                                                                                                                                                                                                                                                                |
| Bytes                   | 🟠 Currently not supported.                                                                                                                                                                                                                                           |
| Int64                   | Integer                                                                                                                                                                                                                                                               |
| Float64                 | Float                                                                                                                                                                                                                                                                 |
| Numeric                 | Float                                                                                                                                                                                                                                                                 |
| BigDecimal / BigNumeric | Float                                                                                                                                                                                                                                                                 |
| Bool                    | Boolean                                                                                                                                                                                                                                                               |
| Date                    | Date                                                                                                                                                                                                                                                                  |
| Datetime                | Datetime                                                                                                                                                                                                                                                              |
| Timestamp               | Datetime                                                                                                                                                                                                                                                              |
| Timestamp without TZ    | Datetime with UTC                                                                                                                                                                                                                                                     |
| Time                    | Time                                                                                                                                                                                                                                                                  |
| Geography               | 🟠 Currently not supported.                                                                                                                                                                                                                                           |
| JSON                    | Struct                                                                                                                                                                                                                                                                |
| Record                  | Struct                                                                                                                                                                                                                                                                |
| Interval                | 🟠 Currently not supported.                                                                                                                                                                                                                                           |
| Struct                  | Struct                                                                                                                                                                                                                                                                |
