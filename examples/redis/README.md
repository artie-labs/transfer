# Redis Destination Example

This example demonstrates how to use Artie Transfer with Redis as a destination.

## Prerequisites

- Redis server (6.0+)
- Kafka cluster
- Debezium connector (or another CDC source) sending data to Kafka

## Quick Start

### 1. Start Redis

Using Docker:
```bash
docker run -d -p 6379:6379 --name redis redis:latest
```

Or with password protection:
```bash
docker run -d -p 6379:6379 --name redis redis:latest redis-server --requirepass mypassword
```

### 2. Configure Transfer

Edit `config.yaml` and update:
- Redis connection settings (host, port, password, database)
- Kafka bootstrap server and topics
- Topic configurations for your tables

### 3. Run Transfer

```bash
./transfer run --config examples/redis/config.yaml
```

## Verifying Data

Connect to Redis and inspect the data:

```bash
# Connect to Redis
redis-cli

# List all keys for a table
KEYS mydb:public:users:*

# Get a specific record
HGETALL mydb:public:users:1

# Get just the data field
HGET mydb:public:users:1 _artie_data

# Count records for a table
EVAL "return #redis.call('keys', 'mydb:public:users:*')" 0

# Get the current counter (next ID)
GET mydb:public:users:__counter
```

Example output:
```
127.0.0.1:6379> HGETALL mydb:public:users:1
1) "_artie_emitted_at"
2) "2025-12-01T10:30:00Z"
3) "_artie_data"
4) "{\"id\":123,\"name\":\"John Doe\",\"email\":\"john@example.com\",\"created_at\":\"2025-01-01T00:00:00Z\"}"
```

## Data Model

Each CDC event creates a new Redis Hash with:
- **Key pattern**: `[database]:[schema]:[table]:[id]`
- **Hash fields**:
  - `_artie_emitted_at`: When Transfer processed this record
  - `_artie_data`: Complete record as JSON

## Configuration Options

### Redis Settings

```yaml
redis:
  host: localhost          # Redis server hostname
  port: 6379              # Redis server port
  password: ""            # Optional password
  database: 0             # Redis database (0-15)
```

### Mode Settings

- **replication**: Real-time CDC replication
- **history**: Maintain complete history of changes

### Flush Settings

```yaml
flushIntervalSeconds: 10  # Flush every 10 seconds
flushSizeKb: 10240       # Flush when buffer reaches 10MB
bufferRows: 10000        # Maximum rows to buffer
```

## Use Cases

1. **Event Streaming**: Store CDC events for real-time processing
2. **Caching Layer**: Use Redis as a fast cache for recent changes
3. **Microservices Integration**: Share CDC data across services
4. **Analytics Pipeline**: Temporary storage before processing
5. **Message Queue**: Durable event storage with Redis persistence

## Monitoring

Monitor your Redis instance:

```bash
# Redis info
redis-cli INFO

# Memory usage
redis-cli INFO memory

# Number of keys
redis-cli DBSIZE

# Monitor live commands
redis-cli MONITOR
```

## Performance Tips

1. **Enable Redis persistence** (RDB or AOF) for durability
2. **Set maxmemory policy** to prevent OOM issues
3. **Monitor memory usage** and plan for growth
4. **Consider Redis Cluster** for horizontal scaling
5. **Use pipelining** (already implemented in Transfer)

## Data Retention

Redis data is persistent by default. For automated cleanup:

### Option 1: Use Redis TTL
Set TTL on keys after creation (requires custom script or Redis module)

### Option 2: Periodic Cleanup
Create a cleanup script:
```bash
#!/bin/bash
# Delete keys older than 7 days
redis-cli --eval cleanup.lua mydb:public:users
```

### Option 3: Use Redis Streams
Consider using Redis Streams for better time-series data management

## Troubleshooting

### Connection Issues
```bash
# Test Redis connectivity
redis-cli -h localhost -p 6379 PING

# With password
redis-cli -h localhost -p 6379 -a mypassword PING
```

### Memory Issues
```bash
# Check memory usage
redis-cli INFO memory

# Get memory usage per key pattern
redis-cli --bigkeys
```

### Performance Issues
```bash
# Check slow queries
redis-cli SLOWLOG GET 10

# Monitor operations
redis-cli MONITOR
```

## Next Steps

- Configure multiple tables in `topicConfigs`
- Set up monitoring and alerts
- Implement data retention policies
- Consider Redis Cluster for production use
- Add backup and recovery procedures

