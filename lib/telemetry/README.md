# Telemetry

The Telemetry package aims to bring a high level of visiblity into the Transfer application by providing metrics and tracing.

_Note: Transfer will use [OpenTelemetry](https://opentelemetry.io/) (which is a vendor netural telemetry framework) once the library is stable._


## Metrics

| Name | Description | Unit of measurement | Available tags |
| ----- | --------------- | ----- | ----------- |
| `transfer.process.message.count` | How many rows has Transfer processed. | Count | `database, schema, table, what, groupid` |
|  `transfer.process.message.95percentile` | p95 of how long each row process takes | `ms` | `database, schema, table, what, groupid` |
|  `transfer.process.message.avg` | Avg of how long each row process takes | `ms` | `database, schema, table, what, groupid` |
|  `transfer.process.message.max` | Max of how long each row process takes | `ms` | `database, schema, table, what, groupid` |
|  `transfer.process.message.median` | Median of how long each row process takes | `ms` | `database, schema, table, what, groupid` |
| `transfer.flush.count` | How many flush operations have been performed. | Count | `database, schema, table, what` |
|  `transfer.flush.95percentile` | p95 of how long each flush process takes | `ms` | `database, schema, table, what` |
|  `transfer.flush.avg` | Avg of how long each flush process takes | `ms` | `database, schema, table, what` |
|  `transfer.flush.max` | Max of how long each flush process takes | `ms` | `database, schema, table, what` |
|  `transfer.flush.median` | Median of how long each flush process takes | `ms` | `database, schema, table, what` |

### The what tag explained

The `what` tag aims to provide a high level of visibility into whether an attempt has succeeded or not. And if it did not succeed, it will provide additional visibility into which particular operation failed (vs just providing a generic error state).

Transfer will provide `what:success` if the attempt failed and different reasoning depending on the erorr state. This way, our monitors and response to failures can be more actionable and we can jump straight to the offending codeblock.

Here's a visualization of what this looks like:
<img width="1397" alt="Screen Shot 2022-12-20 at 2 38 24 PM" src="https://user-images.githubusercontent.com/4412200/208600248-fbf1ee0e-4678-4370-86f8-72dbdfbcbdff.png">

## Tracing
Planned - currently not available.
