---
description: Going over how to find your BigQuery settings and outline the typing support.
---

# BigQuery

## Finding your BigQuery settings

### Getting your project identifier

When you are in your GCP project, you can see your GCP project ID at the top of the navigation bar.

<figure><img src="../.gitbook/assets/image (34).png" alt=""><figcaption></figcaption></figure>

### Getting your default dataset

In order for Transfer to connect to BigQuery, we will need to specify a default dataset.&#x20;

{% hint style="info" %}
The default dataset is only meant to establish the initial connection with BigQuery. The actual dataset specified within `topicConfig` can be different from the default dataset!
{% endhint %}

<figure><img src="../.gitbook/assets/image (15).png" alt=""><figcaption></figcaption></figure>

### Retrieving your credentials

For best practice, create a separate service account for `Transfer` and download the keys by following [this guide](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) from Google!

### Best practices

* For large tables, consider turning on Time Partitioning 👉 [enabling-bigquery-time-partitioning.md](../tutorials/enabling-bigquery-time-partitioning.md "mention")
