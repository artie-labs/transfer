.PHONY: all
all:
	make clean
	make generate
	make test

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
