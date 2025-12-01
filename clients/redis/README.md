# Redis Destination

The Redis destination writes CDC data to Redis using Hash data structures. Each record is stored as a Redis Hash with a unique key.

## Key Features

- **Hash-based storage**: Each record is stored as a Redis Hash
- **Auto-incrementing IDs**: Uses Redis INCR command to generate unique IDs for each record
- **JSON serialization**: Record data is serialized as JSON
- **Namespace support**: Supports database/schema/table organization

## Key Naming Convention

Redis keys follow this pattern:
```
[namespace]:[schema]:[table]:[id]
```

Where:
- **namespace**: The source database name
- **schema**: The source schema name
- **table**: The table/stream name
- **id**: A unique sequential ID generated using Redis INCR

Example: `mydb:public:users:1`, `mydb:public:users:2`, etc.

## Hash Structure

Each Redis Hash contains two fields:

1. **_artie_emitted_at**: ISO 8601 timestamp when the record was processed
2. **_artie_data**: The actual record data stored as a JSON string

Example:
```
HGETALL mydb:public:users:1
1) "_artie_emitted_at"
2) "2025-12-01T10:30:00Z"
3) "_artie_data"
4) "{\"id\":123,\"name\":\"John Doe\",\"email\":\"john@example.com\"}"
```

## Configuration

Add Redis configuration to your `config.yaml`:

```yaml
outputSource: redis

redis:
  host: localhost
  port: 6379
  password: ""  # Optional, leave empty if no password
  database: 0   # Redis database number (0-15)

# Standard Artie Transfer settings
mode: replication
flushIntervalSeconds: 10
flushSizeKb: 10240
bufferRows: 10000

kafka:
  bootstrapServer: localhost:9092
  groupID: transfer
  topicConfigs:
    - db: mydb
      schema: public
      tableName: users
      topic: dbserver.public.users
      cdcFormat: debezium.relational
      cdcKeyFormat: org.apache.kafka.connect.json.JsonConverter
```

## Operations

### Writing Data

The Redis destination handles all CDC operations (Create, Update, Delete) by creating new hash entries. Each operation results in a new record with a unique ID, making it suitable for append-only use cases and event streaming.

### Dropping Tables

The `DropTable` operation will:
1. Scan for all keys matching the table pattern
2. Delete all matching keys
3. Delete the counter key

### Querying Data

You can query Redis data using standard Redis commands:

```bash
# Get all keys for a table
KEYS mydb:public:users:*

# Get a specific record
HGETALL mydb:public:users:1

# Get just the data field
HGET mydb:public:users:1 _artie_data

# Get current counter value (next ID will be counter + 1)
GET mydb:public:users:__counter
```

## Use Cases

Redis destination is ideal for:
- **Event streaming**: Append-only event logs
- **Real-time caching**: Fast access to recent CDC events
- **Message queues**: Using Redis as a durable event store
- **Analytics pipelines**: Temporary storage before processing
- **Microservices integration**: Sharing CDC data across services via Redis

## Limitations

- No SQL query support (use Redis commands instead)
- No deduplication (each event creates a new hash)
- No merge operations (append-only)
- No temporary tables
- Records are not updated in-place; each change creates a new entry

## Performance Considerations

- Uses Redis pipelining for batch writes to improve throughput
- Auto-incrementing counter uses atomic INCR operations
- Consider Redis memory limits when storing large datasets
- Use Redis TTL policies or external cleanup jobs to manage data retention

