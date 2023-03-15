---
description: >-
  In this page, we'll go over how to gather your MongoDB credentials running on
  Atlas.
---

# Atlas - MongoDB

{% hint style="info" %}
In this section, we will need to gather the following values:

* MongoDB Host
* Username
* Password
* Authentication Database

\
Also, to allow Artie Transfer to read the change event stream, the MongoDB cluster must be running in a Replica Set. If you have any questions about how to set that up, get in touch with us at [hi@artie.so](mailto:hi@artie.so)!
{% endhint %}

## MongoDB Host

Provide a comma separated list of hosts within the Replica Set. We recommend this approach since Debezium relies on the primary node within the cluster to stream changes. If the cluster undergoes an election process and elects a new primary, Debezium will automatically re-establish the underlying connection.

<figure><img src="../../.gitbook/assets/image (1).png" alt=""><figcaption></figcaption></figure>

## Authentication

Please see the diagram below. To get here, go into your Atlas console, make sure you are on the right project and then select "Database Access".

<figure><img src="../../.gitbook/assets/image (2).png" alt=""><figcaption></figcaption></figure>
