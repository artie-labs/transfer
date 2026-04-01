# Contributing to Transfer

Thanks for contributing to Artie Transfer. This guide covers how to set up, test, and contribute to the repository.

## Prerequisites

- Go 1.22+
- Docker and Docker Compose
- `make`

## Local Setup

Clone the repository and download dependencies:

```sh
git clone https://github.com/artie-labs/transfer.git
cd transfer
go mod download
```

## Running Locally

To run the full stack locally, bring up the Docker Compose environment which includes Postgres, Kafka, and Zookeeper, then run the app:

```sh
docker compose up -d
go run main.go --config config.yaml
```

## Testing

### Unit Tests
Run standard Go tests:
```sh
go test ./...
```

### E2E Tests
End-to-end testing relies on Docker Compose. Navigate to `e2e_tests/postgres` and run the flow:
```sh
cd e2e_tests/postgres
docker compose up
```

## Code Style

- Format all code with `gofmt -s -w .` before committing
- Run the linter: `golangci-lint run`
- Use short, lowercase error formats: `fmt.Errorf("do thing: %w", err)`
- Log using `slog`, not `fmt` or `log`
- Avoid verbose comments. Document the "why", not the "what"

**Commit format:** `[Component] Description (#PR)`
Examples:
- `[Kafka] Remove kafka-go (#1620)`
- `[Iceberg] Supporting REST Catalog (#1626)`

## Adding a New Destination

If you are adding a new data warehouse or destination:

1. Implement the `destination.DataWarehouse` interface. Create your implementation under `clients/your_destination/client.go`.
2. Add the configuration struct in `lib/config/your_destination.go`.
3. Register the new destination constant in `lib/config/constants/constants.go`.
4. Wire your config to the main `Config` struct in `lib/config/config.go`.
5. Return your initialized client in `lib/destination/utils/load.go`.

## Adding a New Queue Source

To add a new message queue:

1. Implement the `Consumer` interface (see `lib/kafkalib/consumer.go`). Place your implementation in `lib/yourqueuelib/consumer.go`.
2. Add the queue type constant to `QueueKind` in `lib/config/constants/constants.go` and update validation checks.
3. Wire the queue configuration in `lib/config/config.go`.
4. Create a specific runner for the queue in `processes/consumer/your_queue.go`.
5. Dispatch the runner in `main.go`.

## PR Process

1. Open an issue first before large features
2. Write unit tests for your changes
3. Ensure CI passes
4. Wait for code review
