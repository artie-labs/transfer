# Overview

In this section, we will go over the metrics that Artie Transfer emits and the future roadmap.

## Today

Artie Transfer's currently only supports metrics and integrates with Datadog. We are committed to being vendor neutral, but not at the cost of reliability. As such, we will be using [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go#project-status) when the library is stable.&#x20;

We also plan to support application tracing such that we can directly plug into your APM provider.

## Metrics

> You can specify additional tags and namespace in the configuration file and it will apply to every metric that Transfer emits. See [options.md](../running-transfer/options.md "mention") for more details.

| Name                                    | Description                                    | Unit  | Tags                                                                    |
| --------------------------------------- | ---------------------------------------------- | ----- | ----------------------------------------------------------------------- |
| `transfer.process.message.count`        | How many rows has Transfer processed.          | Count | <ul><li>database</li><li>schema</li><li>table</li><li>groupid</li></ul> |
| `transfer.process.message.95percentile` | p95 of how long each row process takes         | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>groupid</li></ul> |
| `transfer.process.message.avg`          | Avg of how long each row process takes         | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>groupid</li></ul> |
| `transfer.process.message.max`          | Max of how long each row process takes         | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>groupid</li></ul> |
| `transfer.process.message.median`       | Median of how long each row process takes      | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>groupid</li></ul> |
| `transfer.flush.count`                  | How many flush operations have been performed. | Count | <ul><li>database</li><li>schema</li><li>table</li><li>what</li></ul>    |
| `transfer.flush.95percentile`           | p95 of how long each flush process takes       | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>what</li></ul>    |
| `transfer.flush.avg`                    | Avg of how long each flush process takes       | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>what</li></ul>    |
| `transfer.flush.max`                    | Max of how long each flush process takes       | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>what</li></ul>    |
| `transfer.flush.median`                 | Median of how long each flush process takes    | `ms`  | <ul><li>database</li><li>schema</li><li>table</li><li>what</li></ul>    |

## The what tag explained

The `what` tag aims to provide a high level of visibility into whether an attempt has succeeded or not. And if it did not succeed, it will provide additional visibility into which particular operation failed (vs just providing a generic error state).

Transfer will provide `what:success` if the attempt failed and different reasoning depending on the error state. This way, our monitors and response to failures can be more actionable and we can jump straight to the offending code block.

<figure><img src="../.gitbook/assets/image.png" alt=""><figcaption><p>Here's a visualization of what this looks like</p></figcaption></figure>

