BIN := indexer-reference-provider

.PHONY: all build clean test

all: build

build: $(BIN)

docker: Dockerfile clean
	docker build . --force-rm -f Dockerfile -t indexer-reference-provider:$(shell git rev-parse --short HEAD)

$(BIN): vet test
	go build -o $@ cmd/provider/*.go

lint:
	golangci-lint run

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -f $(BIN)
	go clean
