BIN := index-provider
CMD_SUBDIR := cmd
TMP_MODFILE = $(CMD_SUBDIR)/go-tmp.mod
TMP_SUMFILE = $(CMD_SUBDIR)/go-tmp.sum

.PHONY: all build clean test

all: build

build: $(BIN)

docker: Dockerfile clean
	docker build . --force-rm -f Dockerfile -t indexer-reference-provider:$(shell git rev-parse --short HEAD)

# Build index-provider executable.  Use a temporary cmd/go.mod to build using
# code from the current project directory.
$(BIN): vet test
	@cp -f $(CMD_SUBDIR)/go.mod $(TMP_MODFILE)
	@cp -f $(CMD_SUBDIR)/go.sum $(TMP_SUMFILE)
	@go mod edit -modfile $(TMP_MODFILE) -replace github.com/filecoin-project/index-provider=./
	@go build -modfile $(TMP_MODFILE) -o $@ $(CMD_SUBDIR)/provider/*.go
	@rm -f $(TMP_MODFILE) $(TMP_SUMFILE)
	@echo "===> Built $@"

lint:
	golangci-lint run

mock/interface.go: interface.go
	mockgen --source interface.go --destination mock/interface.go --package mock_provider

test: mock
	go test ./...

vet:
	go vet ./...

clean:
	rm -f $(BIN) $(TMP_MODFILE) $(TMP_SUMFILE)
	go clean
