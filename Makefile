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
	go get github.com/maxbrunsfeld/counterfeiter/v6
	go generate ./...
	go mod tidy
.PHONY: build
build:
	goreleaser build --clean

.PHONY: release
release:
	goreleaser release --clean

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
