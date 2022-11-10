.PHONY: all
all:
	make clean
	make generate
	make test

.PHONY: test
test:
	go test ./...

.PHONY: clean
clean:
	go clean -testcache

.PHONY: generate
generate:
	go generate ./...

