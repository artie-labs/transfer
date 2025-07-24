.PHONY: all
all:
	make clean
	make generate
	make test

.PHONY: static
static:
	go tool staticcheck ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

.PHONY: race
race:
	go test -race ./...

.PHONY: clean
clean:
	go clean -testcache

.PHONY: generate
generate:
	cd lib/mocks && go tool counterfeiter -generate

.PHONY: build
build:
	goreleaser build --clean

.PHONY: release
release:
	goreleaser release --clean

.PHONY: outdated
outdated:
# Note this will not output major version changes of dependencies.
	go list -u -m -f '{{if and .Update (not .Indirect)}}{{.}}{{end}}' all

.PHONY: bench_size
bench_size:
	go test ./lib/size -bench=Bench -benchtime=10s
	go test ./lib/optimization -bench=Bench -benchtime=10s

.PHONY: bench_typing
bench_typing:
	go test ./lib/typing/... -bench=Bench -benchtime=20s
	go test ./lib/debezium -bench=Bench -benchtime=20s

.PHONY: bench_redshift
bench_redshift:
	go test ./clients/redshift -bench=Bench -benchtime=20s

.PHONY: bench_mongo
bench_mongo:
	go test ./lib/cdc/mongo -bench=Bench -benchtime=20s

.PHONY: dest-itest-append
dest-itest-append:
	go run integration_tests/destination_append/main.go --config .personal/integration_tests/snowflake.yaml
	go run integration_tests/destination_append/main.go --config .personal/integration_tests/bigquery.yaml
	go run integration_tests/destination_append/main.go --config .personal/integration_tests/databricks.yaml
	go run integration_tests/destination_append/main.go --config .personal/integration_tests/redshift.yaml
	go run integration_tests/destination_append/main.go --config .personal/integration_tests/mssql.yaml
	go run integration_tests/destination_append/main.go --config .personal/integration_tests/iceberg.yaml

.PHONY: dest-itest-merge
dest-itest-merge:
	go run integration_tests/destination_merge/main.go --config .personal/integration_tests/snowflake.yaml
	go run integration_tests/destination_merge/main.go --config .personal/integration_tests/bigquery.yaml
	go run integration_tests/destination_merge/main.go --config .personal/integration_tests/databricks.yaml
	go run integration_tests/destination_merge/main.go --config .personal/integration_tests/redshift.yaml
	go run integration_tests/destination_merge/main.go --config .personal/integration_tests/mssql.yaml

.PHONY: parquet-venv
parquet-venv:
	@echo "Setting up Python venv for parquet integration test..."
	@if [ ! -d integration_tests/parquet/venv ]; then \
		python3 -m venv integration_tests/parquet/venv; \
	fi
	@integration_tests/parquet/venv/bin/pip install --upgrade pip > /dev/null
	@integration_tests/parquet/venv/bin/pip install -r integration_tests/parquet/requirements.txt > /dev/null

.PHONY: test-parquet
test-parquet: parquet-venv
	@cd integration_tests/parquet && go run main.go
	@echo "Running parquet verification (Python)..."
	@cd integration_tests/parquet && venv/bin/python verify_parquet.py

.PHONY: dest-itest-types
dest-itest-types:
	go run integration_tests/destination_types/main.go --config .personal/integration_tests/redshift.yaml
	go run integration_tests/destination_types/main.go --config .personal/integration_tests/mssql.yaml
	go run integration_tests/destination_types/main.go --config .personal/integration_tests/snowflake.yaml

.PHONY: postgres-itest
postgres-itest:
	go run integration_tests/postgres/main.go
