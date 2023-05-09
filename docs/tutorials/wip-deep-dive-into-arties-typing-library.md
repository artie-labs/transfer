---
description: >-
  Ever wonder how Artie's typing library works? You've found the right article!
  In this section, we will discuss how Artie's internal typing library works and
  how we ensure source-data integrity.
---

# \[WIP] Deep-dive into Artie's typing library

## Context

We created our own [typing library](https://github.com/artie-labs/transfer/tree/master/lib/typing) that empowers Artie to provide schema evolution support and ensure the data you see in your [source](broken-reference) looks exactly the same in your [destination](broken-reference).

## Ints vs. Floats

When in doubt, we follow the same logic as `JSON encoding` which is to default to `FLOAT` whenever we are in-doubt. We then have an ability to allow sources to [specify an optional schema](https://github.com/artie-labs/transfer/blob/a30cf5c67a699ba8bcf1e483aa7535ad818b6af9/lib/debezium/schema.go#L44-L51) that will override our typing library in thinking this is a `INT` when there is a schema available. This is automatically supported for all of our sources.

> What happens if the number is `5` vs `"5`"?

The first one, `5` will get casted as a `FLOAT` and the second one will be casted a `STRING`.&#x20;

## JSON objects

> A common question that we get is whether or not we apply any sort of transform such as JSON object flattening.&#x20;

As part of our values to preserve data integrity, we will provide the JSON object back out to the destination in the way that it was received. See below for an example in Snowflake.

<figure><img src="../.gitbook/assets/image.png" alt=""><figcaption><p>JSON object preservation in Snowflake example</p></figcaption></figure>

> How do you know if a value is a JSON object?

Our typing library will try to run values through our JSON parser and label this value as a JSON object if it passes our parsing test.&#x20;

If you explicitly want this to be stored as a `JSON string`, you [can also explicitly specify](https://github.com/artie-labs/transfer/blob/a30cf5c67a699ba8bcf1e483aa7535ad818b6af9/lib/typing/typing.go#L83-L89) this column to be a `STRING`. This is automatically supported for our all of our sources.

