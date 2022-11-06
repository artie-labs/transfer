.PHONY: test
test:
	go test ./...

.PHONY: generate
generate:
	go generate ./...
