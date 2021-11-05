Indexer Reference Provider :loudspeaker:
=======================
[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![Go Reference](https://pkg.go.dev/badge/github.com/filecoin-project/indexer-reference-provider.svg)](https://pkg.go.dev/github.com/filecoin-project/indexer-reference-provider)
[![Coverage Status](https://codecov.io/gh/filecoin-project/indexer-reference-provider/branch/main/graph/badge.svg)](https://codecov.io/gh/filecoin-project/indexer-reference-provider/branch/main)

> A reference implementation of indexer data provider

This repo provides a reference data provider implementation that can be used to advertise content to
indexer nodes and serve retreival requests over graphsync.

## Current status :construction:

This implementation is under active development.

## Install

Prerequisite:

- [Go 1.16+](https://golang.org/doc/install)

To use the provider as a Go library, execute:

```shell
go get github.com/filecoin-project/indexer-reference-provider
```

To install the latest runnable version of the provider service, execute:

```shell
go install github.com/filecoin-project/indexer-reference-provider/cmd/provider@latest
```

## Running Provider Service

To run a provider service it must first be initialized. To initialize the provider, execute:

```shell
provider init
```

Initialization generates a default configuration for the provider instance along with a randomly
generated identity keypair. The configuration is stored at user home
under `.reference-provider/config` in JSON format. The root configuration path can be overridden by
setting the `PROVIDER_PATH` environment variable

Once initialized, start the service daemon by executing:

```shell
provider daemon
```

The running daemon allows advertisement of new content to the indexer nodes and retrieval of content
over graphsync. Additionally, it starts an admin HTTP server that enables administrative operations
using the `provider` CLI tool. By default, the admin server is bound to `http://localhost:3102`.

## `provider` CLI

The `provider` CLI can be used to interact with a running daemon via the admin server to perform a
range of administrative operations. For example, the `provider` CLI can be used to import a CAR file
and advertise its content to the indexer nodes by executing:

```shell
provider import car -l http://localhost:3102 -i <path-to-car-file>
```

For full usage, execute `provider`. Usage:

````shell
NAME:
   provider - Indexer Reference Provider Implementation

USAGE:
   provider [global options] command [command options] [arguments...]

VERSION:
   v0.0.0+unknown

COMMANDS:
   daemon     Starts a reference provider
   find       Query an indexer for indexed content
   index      Push a single content index into an indexer
   init       Initialize reference provider config file and identity
   connect    Connects to an indexer through its multiaddr
   import, i  Imports sources of multihashes to the index provider.
   register   Register provider information with an indexer that trusts the provider
   help, h    Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help (default: false)
   --version, -v  print the version (default: false)
````

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
