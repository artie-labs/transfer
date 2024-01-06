---
description: 'Published: Nov 27, 2023'
---

# November 2023

Welcome to our inaugural product update where we will highlight major product releases, new connectors, and new features.

## Major Releases & News

### Analytics Portal

We are extremely excited to announce our Analytics Portal to increase visibility and observability of our streaming pipelines. This will provide insights into system level infrastructure and help with monitoring database and pipeline health. When identifying and resolving issues, one of the most important metrics is to reduce MTTD (mean time to detection). With Artie’s streaming pipelines and periodic jobs like [Postgres Watcher](https://docs.artie.so/real-time-sources/postgresql#postgresql-watcher), metrics are being sent to our Analytics Portal in-flight, as the underlying data is still being processed.&#x20;

<figure><img src="../.gitbook/assets/image (39).png" alt=""><figcaption></figcaption></figure>

With the first iteration of our Analytics Portal, we are providing industry standard telemetry to streaming pipelines and related infrastructure. The Analytics Portal initially comes with a set of pre-built charts and monitors. Customers are able to drill down to get deployment, database, and table level statistics.&#x20;

Pre-built monitors that we are launching with include alerts for database permission errors and replication slot growth (for Postgres users). Over time, we will add alerting for other monitors we mention above and more.&#x20;

Read more about our Analytics Portal [here](https://blog.artie.so/introducing-arties-analytics-portal).

### SOC 2 Type II Compliance

We achieved SOC 2 Type II compliance this month! Read the full announcement [here](https://blog.artie.so/artie-is-soc-2-type-ii-compliant).

## New Features & Upgrades

* PostgreSQL Watcher is automatically turned on for customers. Postgres users will receive emails on replication slot growth and database permission errors.
* Ability to skip deletes at the table level. In addition to having an ability to perform soft and hard deletes, you can now skip deletes altogether.
* Ability to rename tables on the dashboard. If the source table is named \`foo\`, you can rename it to be \`bar\` in your downstream data warehouse.
* Ability to remove team members from the admin dashboard.
* New data plane set up in US-West-2. Customers that are located in the US-West-2 region have been migrated over.&#x20;
* MongoDB SRV connection string format is now supported.
* Our dashboard now loads faster! We reduced our customer-facing dashboard builds from 21 MB to 5.3 MB.

## Blogs

* [Best Practices on Running Redshift at Scale](https://blog.artie.so/best-practices-on-running-redshift-at-scale)
* [Preventing WAL growth on Postgres running on AWS RDS](https://blog.artie.so/preventing-wal-growth-on-postgres-db-running-on-aws-rds)
* [Snowflake Eco Mode](https://blog.artie.so/snowflake-eco-mode) - minimize time and maximize resource utilization on Snowflake

\
