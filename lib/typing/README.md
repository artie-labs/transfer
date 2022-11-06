# Typing

Typing is a core utility within Transfer, as such - we have created a lot of utilities and strayed away from using other client libraries as much as possible.

Once our schema detection detects a change, we will need to take the first not-null value from the CDC stream and infer the type.
This is where our library comes in:
* We will figure out the type (we support a variety of date time formats)
* Based on the type, we will then call DWH and create a column with the inferred type.
* This is necessary as there are transactional DBs that are schemaless (MongoDB, Bigtable, DynamoDB to name a few...)

## Performance

As part of this being a core utility within Artie, we decided to write our own Typing library. <br/>
Below, you can see the difference between Artie and Reflect (which is our baseline).

```
> make

BenchmarkParseValueIntegerArtie-8   	1000000000	         2.804 ns/op
BenchmarkParseValueIntegerGo-8      	1000000000	         4.788 ns/op
BenchmarkParseValueBooleanArtie-8   	1000000000	         2.656 ns/op
BenchmarkParseValueBooleanGo-8      	1000000000	         5.042 ns/op
BenchmarkParseValueFloatArtie-8     	1000000000	         2.684 ns/op
BenchmarkParseValueFloatGo-8        	1000000000	         4.784 ns/op
```
