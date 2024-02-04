---
description: >-
  Databases managed by Supabase have logical replication enabled already. We
  will just need to find the connection string for Artie to connect and start
  replicating.
---

# Supabase

{% hint style="warning" %}
For Artie to integrate with Supabase, we **need** to use **IPv4.** This is available as an add-on. Click [here](https://github.com/orgs/supabase/discussions/17817) to read more.
{% endhint %}

## Enabling IPv4

To do this, navigate to `Settings` > `Add Ons` > `IPv4 Address`

## Finding your database credentials

To do this, go to `Settings` > `Database` > `Connection Parameters`

Make sure the connection string is using IPv4 and no connection pooling.

