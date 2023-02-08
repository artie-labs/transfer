# Telemetry

In this section, we will go over the metrics that Artie Transfer emits and the future roadmap.

## Today

Artie Transfer's currently only supports metrics and integrates with Datadog. We are committed to being vendor neutral, but not at the cost of reliability. As such, we will be using [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go#project-status) when the library is stable.&#x20;

We also plan to support application tracing such that we can directly plug into your APM provider.

## Metrics

| Name                                    | Description                                    | Unit  | Available tags                           |
| --------------------------------------- | ---------------------------------------------- | ----- | ---------------------------------------- |
| `transfer.process.message.count`        | How many rows has Transfer processed.          | Count | `database, schema, table, what, groupid` |
| `transfer.process.message.95percentile` | p95 of how long each row process takes         | `ms`  | `database, schema, table, what, groupid` |
| `transfer.process.message.avg`          | Avg of how long each row process takes         | `ms`  | `database, schema, table, what, groupid` |
| `transfer.process.message.max`          | Max of how long each row process takes         | `ms`  | `database, schema, table, what, groupid` |
| `transfer.process.message.median`       | Median of how long each row process takes      | `ms`  | `database, schema, table, what, groupid` |
| `transfer.flush.count`                  | How many flush operations have been performed. | Count | `database, schema, table, what`          |
| `transfer.flush.95percentile`           | p95 of how long each flush process takes       | `ms`  | `database, schema, table, what`          |
| `transfer.flush.avg`                    | Avg of how long each flush process takes       | `ms`  | `database, schema, table, what`          |
| `transfer.flush.max`                    | Max of how long each flush process takes       | `ms`  | `database, schema, table, what`          |
| `transfer.flush.median`                 | Median of how long each flush process takes    | `ms`  | `database, schema, table, what`          |



