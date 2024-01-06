---
description: >-
  Curious how Artie's typing library works? You've come to the right place!
  Here, we will discuss how Artie's internal typing library works and how we
  ensure source-data integrity.
---

# Artie's typing library

## Context

We created our own [typing library](https://github.com/artie-labs/transfer/tree/master/lib/typing) that empowers Artie to provide schema evolution support and ensure the data you see in your [source](broken-reference) looks exactly the same in your [destination](broken-reference).&#x20;

At a high level, Transfer will detect missing columns and automatically add them to the destination and set the correct types. When we get the first `NOT NULL` value, we will run this through our typing library so that we can infer the correct data type. Transfer strives to maintain source-data integrity and as a result, we do not apply any transforms. The second objective with our typing library is also performance, as a slow typing library may incur additional overhead for row processing. We are happy to report that our [typing library is 2x faster](https://github.com/artie-labs/transfer/tree/master/lib/typing) than Go's Reflect library.

## Numbers

Our schema conforms directly to the source (Postgres, MySQL and MongoDB), so we will set the data type (whether it be a FLOAT or INT) correctly to our destination.

> What happens if the value is `5` vs `"5`"?

The first one, `5` , will be casted as a `INT` and the second one will be casted a `STRING`.&#x20;

## JSON objects

> A common question that we get is whether or not we apply any sort of transform such as JSON object flattening.&#x20;

As part of our values to preserve data integrity, we will provide the JSON object back out to the destination in the way that it was received. See below for an example in Snowflake.

<figure><img src="../.gitbook/assets/image (8).png" alt=""><figcaption><p>JSON object preservation in Snowflake example</p></figcaption></figure>

> How do you know if a value is a JSON object?

Our typing library will try to run values through our JSON parser and label this value as a JSON object if it passes our parsing test.&#x20;

If you explicitly want this to be stored as a `JSON string`, you [can also explicitly specify](https://github.com/artie-labs/transfer/blob/a30cf5c67a699ba8bcf1e483aa7535ad818b6af9/lib/typing/typing.go#L83-L89) this column to be a `STRING`. This is automatically supported for all of our sources.

## Arrays

Arrays also have first-class support and we support the following:

* Normal arrays
* Nested arrays

<figure><img src="../.gitbook/assets/image (32).png" alt=""><figcaption><p>Normal array</p></figcaption></figure>

<figure><img src="../.gitbook/assets/image (12).png" alt=""><figcaption><p>Array with nested objects</p></figcaption></figure>

## Timestamp, Date and Time

We support \~15 [different formats](https://github.com/artie-labs/transfer/blob/master/lib/typing/ext/variables.go#L13) across these data types with zero precision loss. We also have our own `time.Time` object which keeps your original layout when replaying to your destination.&#x20;

Similar to JSON objects, if you do not want the Typing library to infer your string value as a `TIMESTAMP`, `DATE` or `TIME`, then simply pass the preferred data type as part of the optional schema. This is automatically supported with all of our sources.

## Is your question not listed here?

If your question is not answered by this page, please reach out to [hi@artie.so](mailto:hi@artie.so)!
