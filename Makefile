BIN_SUBDIR := cmd/provider

.PHONY: all build clean test

all: vet test build

build: 
	cd $(BIN_SUBDIR) && go build

docker: Dockerfile clean
	docker build . --force-rm -f Dockerfile -t indexer-reference-provider:$(shell git rev-parse --short HEAD)

install:
	cd $(BIN_SUBDIR) && go install

lint:
	golangci-lint run

mock/interface.go: interface.go
	mockgen --source interface.go --destination mock/interface.go --package mock_provider

test: mock
	go test ./...

vet:
	go vet ./...

clean:
	rm -f $(BIN)
	go clean
