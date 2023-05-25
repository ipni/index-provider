MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.PHONY: all
all: vet test build

.PHONY: build
build:
	go build ./cmd/provider 

.PHONY: docker
docker: clean
	docker build . --force-rm -f Dockerfile -t index-provider:$(shell git rev-parse --short HEAD)

.PHONY: install
install:
	go install ./cmd/provider

.PHONY: lint
lint:
	golangci-lint run

.PHONY: mock/interface.go
mock/interface.go: interface.go
	mockgen --source interface.go --destination mock/interface.go --package mock_provider

.PHONY: test
test: mock
	go test ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: clean
clean:
	rm -f provider
	go clean
