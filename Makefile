BIN_SUBDIR := cmd/provider

.PHONY: all build clean test

all: vet test build

build: TAG?=$(shell git describe --tags --abbrev=0)
build: COMMIT?=$(shell git rev-parse HEAD)
build: CLEAN?=$(shell git diff --quiet --exit-code || printf '-unclean')
build:
	cd $(BIN_SUBDIR) && go build -ldflags="-X 'main.version=$(TAG)-$(COMMIT)$(CLEAN)'"

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
