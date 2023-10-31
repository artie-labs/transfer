---
description: >-
  In this page, we'll go over how to gather your MongoDB credentials running on
  Atlas.
---

# Atlas - MongoDB

{% hint style="info" %}
In this section, we will need to gather the following values:

* Connection string
* Username
* Password
* Authentication Database

Also, to allow Artie Transfer to read the change event stream, the MongoDB cluster must be running in a Replica Set. If you have any questions about how to set that up, get in touch with us at [hi@artie.so](mailto:hi@artie.so)!
{% endhint %}

## Connection string

To grab the connection string, follow these steps:

1. Go to [Atlas UI](https://cloud.mongodb.com/)
2. Find your deployment and click `Connect`
3. Click on Shell and we support [SRV and standard connection string](https://www.mongodb.com/docs/manual/reference/connection-string/).



<div align="center">

<figure><img src="../../.gitbook/assets/image (10).png" alt="" width="563"><figcaption><p>SRV connection string</p></figcaption></figure>

</div>

<figure><img src="../../.gitbook/assets/image (11).png" alt="" width="563"><figcaption><p>Standard connection string</p></figcaption></figure>

## Authentication

Please see the diagram below. To get here, go into your Atlas console, make sure you are on the right project and then select "Database Access".

<figure><img src="../../.gitbook/assets/image (2) (2) (1).png" alt=""><figcaption></figcaption></figure>
