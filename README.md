Index Provider :loudspeaker:
============================
[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![Go Reference](https://pkg.go.dev/badge/github.com/filecoin-project/index-provider.svg)](https://pkg.go.dev/github.com/filecoin-project/index-provider)
[![Coverage Status](https://codecov.io/gh/filecoin-project/index-provider/branch/main/graph/badge.svg)](https://codecov.io/gh/filecoin-project/index-provider/branch/main)

> A golang implementation of index provider

This repo provides a reference index provider implementation that can be used to advertise content
to indexer nodes and serve retrieval requests over graphsync both as a standalone service or
embedded into an existing Golang application via a reusable library.

A list of features include:

* [`provider`](cmd/provider) CLI that can:
    * Run as a standalone provider daemon instance.
    * Generate and publish indexing advertisements directly from CAR files.
    * Serve retrieval requests for the advertised content over GraphSync.
    * list advertisements published by a provider instance
    * verify ingestion of multihashes by an indexer node from CAR files, detached CARv2 indices or
      from an index provider's advertisement chain.
* A Golang SDK to embed indexing integration into existing applications, which includes:
    * Programmatic advertisement for content via index provider [Engine](engine) with built-in
      chunking functionality
    * Announcement of changes to the advertised content over GossipSub
      using [`go-legs`](https://github.com/filecoin-project/go-legs)
    * `MultihashLister` integration point for fully customizable look up of advertised multihashes.
    * Utilities to advertise multihashes directly [from CAR files](supplier/car_supplier.go)
      or [detached CARv2 index](index_mh_iter.go) files.
    * Index advertisement [`metadata`](metadata) schema for retrieval
      over [graphsync](metadata/metadata.go) and [bitswap](metadata/bitswap.go)

## Current status :construction:

This implementation is under active development.

## Background

The protocol implemented by this repository is the index provider portion of a larger indexing
protocol
documented [here](https://www.notion.so/protocollabs/Indexer-Node-Design-4fb94471b6be4352b6849dc9b9527825)
. The indexer node implementation can be found
at [`storetheindex`](https://github.com/filecoin-project/storetheindex) repository.

For more details on the ingestion protocol itself
see [Providing data to a network indexer](https://github.com/filecoin-project/storetheindex/blob/main/doc/ingest.md)
.

## Install

Prerequisite:

- [Go 1.16+](https://golang.org/doc/install)

To use the provider as a Go library, execute:

```shell
go get github.com/filecoin-project/index-provider
```

To install the latest `provider` CLI, run:
<!-- 
Note: installation instructions uses `git clone` because the `cmd` module uses `replace` directive 
and cannot be installed directly via `go install`
-->

```shell
go install github.com/filecoin-project/index-provider/cmd/provider@latest
```

Alternatively, download the executables directly from
the [releases](https://github.com/filecoin-project/index-provider/releases).

## Usage

### Running an standalone provider daemon

To run a provider service first initialize it by executing:

```shell
provider init
```

Initialization generates a default configuration for the provider instance along with a randomly
generated identity keypair. The configuration is stored at user home under `.index-provider/config`
in JSON format. The root configuration path can be overridden by setting the `PROVIDER_PATH`
environment variable

Once initialized, start the service daemon by executing:

```shell
provider daemon
```

The running daemon allows advertisement for new content to the indexer nodes and retrieval of
content over GraphSync. Additionally, it starts an admin HTTP server that enables administrative
operations using the `provider` CLI tool. By default, the admin server is bound
to `http://localhost:3102`.

You can then advertise content by importing/removing CAR files via the `provider` CLI, for example:

```shell
provider import car -l http://localhost:3102 -i <path-to-car-file>
```

Both CARv1 and CARv2 formats are supported. Index is regenerated on the fly if one is not present.

#### Exposing reframe server from provider (experimental)

Provider can export a reframe server. [Reframe](https://github.com/ipfs/specs/blob/main/reframe/REFRAME_PROTOCOL.md) is a protocol 
that allows IPFS nodes to advertise their contents to indexers alongside DHT. Reframe server is off by default. 
To enable it, add the following configuration block to the provider config file.

```
{
  ...
  Reframe {
    ListenMultiaddr: "/ip4/0.0.0.0/tcp/50617 (example)"
  }
  ...
}
```

### Embedding index provider integration

The [root go module](go.mod) offers a set of reusable libraries that can be used to embed index
provider support into existing application. The core [`provider.Interface`](interface.go) is
implemented by [`engine.Engine`](engine/engine.go).

The provider `Engine` exposes a set of APIs that allows a user to programmatically announce the
availability or removal of content to the indexer nodes referred to as “advertisement”.
Advertisements are represented as an IPLD DAG, chained together via a link to the previous
advertisement. An advertisement effectively captures the "diff" of the content that is either added
or is no longer provided.

Each advertisement contains:

* Provider ID: the libp2p peer ID of the content provider.
* Addresses: a list of addresses from which the content can be retrieved.
* [Metadata](metadata): a blob of bytes capturing how to retrieve the data.
* Entries: a link pointing to a list of chunked multihashes.
* Context ID: a key for the content being advertised.
* IsRm: flag that tells whether this advertisement is for removal of the previously published content.
* ExtendedProviders: an optional field that is explained in the next section. 

The Entries link points to the IPLD node that contains a list of mulitihashes being advertised. The 
list is represented as a chain of "Entry Chunk"s where each chunk contains a list of multihashes and
a link to the next chunk. This is to accommodate pagination for a large number of multihashes.

The engine can be configured to dynamically look up the list of multihashes that correspond to the
context ID of an advertisement. To do this, the engine requires a `MultihashLister` to be 
registered. The `MultihashLister` is then used to look up the list of multihashes associated to a 
content advertisement. 

`NotifyPut` and `NotifyRemove` are convinience wrappers on top of `Publish` that aim to help to construct advertisements. 
They take care of such things as generating entry chunks, linking to the last published advertisement, signing and others. 
`NotifyPut` can be also used to update metadata for a previously published advertisement 
(for example in the case when a protocol has changed). That can be done by invoking `NotifyPut` with the same context ID 
but different metadata field. `ErrAlreadyAdvertised` will be returned if both context ID and metadata have stayed the same.

For an example on how to start up a provider engine, register a lister and 
advertise content, see:

* [`engine/example_test.go`](engine/example_test.go)

#### Publishing advertisements with extended providers

[Extended providers](https://github.com/filecoin-project/storetheindex/blob/main/doc/ingest.md#extendedprovider) 
field allows for specification of provider families, in cases where a provider operates multiple PeerIDs, perhaps 
with different transport protocols between them, but over the same database of content. 

`ExtendedProviders` can either be applied at the *chain-level* (for all previous and future CIDs published by a provider) or at 
a *context-level* (for CIDs belonging to the specified context ID). That behaviour is set by `ContextID` field.
Multiple different behaviours can be triggered by a combination of `ContextID`, `Metadata`, `ExtendedProviders` and `Override` fields. 
For more information see the [specification](https://github.com/filecoin-project/storetheindex/blob/main/doc/ingest.md#extendedprovider) 

Advertisements with `ExtendedProviders` can be composed manually or by using a convenience `ExtendedProvidersAdBuilder` 
and will have to be signed by the main provider as well as by all `ExtendedProviders`' identities. 
Private keys for these identities have to be provided in the `xproviders.Info` 
(objects)[https://github.com/filecoin-project/index-provider/blob/main/engine/xproviders/xproviders.go] and  
`ExtendedProvidersAdBuilder` will take care of the rest.

> Identity of the main provider will be added to the extended providers list automatically and should not be passed in explicitly. 

Some examples can be found below (assumes the readers familiriaty with the 
(specification)[https://github.com/filecoin-project/storetheindex/blob/main/doc/ingest.md#extendedprovider]).

Publishing an advertisement with context-level `ExtendedProviders`, that will be returned only for CIDs from the specified context ID:
```
  adv, err := ep.NewExtendedProviderAdBuilder(providerID, priv, addrs).
    WithContextID(contextID). 
    WithMetadata(metadata). 
    WithOverride(override). 
    WithExtendedProviders(extendedProviders). 
    WithLastAdID(lastAdId). 
    BuildAndSign()

  if err != nil {
    //...
  }

  engine.Publish(ctx, *adv)
)
```
Constructing an advertisement with chain-level `ExtendedProviders`, that will be returned for every past and future CID published by the main provider:
```
  adv, err := ep.NewExtendedProviderAdBuilder(providerID, priv, addrs). 
    WithMetadata(metadata). 
    WithExtendedProviders(extendedProviders).
    WithLastAdID(lastAdId). 
    BuildAndSign()
)
```
`ExtendedProviders` can also be used to add a new metadata to all CIDs published by the main provider. Such advertisement need to be constructed 
without specifying `ExtendedProviders` at all. The identity of the main provider will be added to the `ExtendedProviders` list by the builder automatically,
so the resulting advertisement will contain only the main provider in the `ExtendedProviders` list (yes, this is also possible:).
That can be used for example to advertise new endpoint with a new protocol alongside the previously advertised one:
```
  adv, err := ep.NewExtendedProviderAdBuilder(providerID, priv, addrs). 
    WithMetadata(metadata). 
    WithLastAdID(lastAdId). 
    BuildAndSign()
)
```
On ingestion, previously published `ExtendedProviders` get overwritten (not merged!) by the newer ones. So in order to update `ExtendedProviders`, 
just publish a new chain/context-level advertisement with the required changes.

Examples of constructing advertisements with `ExtendedProviders` can be found 
(here)[https://github.com/filecoin-project/index-provider/blob/main/engine/xproviders/xproviders_test.go].

### `provider` CLI

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
   v0.2.7

COMMANDS:
   daemon             Starts a reference provider
   find               Query an indexer for indexed content
   index              Push a single content index into an indexer
   init               Initialize reference provider config file and identity
   connect            Connects to an indexer through its multiaddr
   import, i          Imports sources of multihashes to the index provider.
   register           Register provider information with an indexer that trusts the provider
   remove, rm         Removes previously advertised multihashes by the provider.
   verify-ingest, vi  Verifies ingestion of multihashes to an indexer node from a CAR file or a CARv2 Index
   list               Lists advertisements
   help, h            Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help (default: false)
   --version, -v  print the version (default: false)
````

## Storage Consumption

The index provider [engine](engine/engine.go) uses a given datastore to persist two general category
of data:

1. Internal advertisement mappings, and
2. Chunked entries chain cache

If the datastore passed to the engine is reused, it is recommended to wrap it in a namespace prior
to instantiating the engine.

### Internal advertisement mappings

The internal advertisement mappings are purely used by the engine to efficiently handle publication
requests. It generally includes:

- mapping to the latest advertisement
- mappings between advertisement CIDs, their context ID and their corresponding metadata.

The storage consumed by such mappings is negligible and grows linearly as a factor of the number of
advertisements published.

### Chunked entries chain cache

This category stores chunked entries generated by publishing an advertisement with a never seen
before context ID. The chunks are stored in an LRU cache, the maximum size of which is configured by
the following configuration parameters
in [`Ingest`](https://pkg.go.dev/github.com/filecoin-project/index-provider@v0.2.6/config#Ingest)
config:

- `LinkChunkSize` - The maximum number of multihashes in a chunk (defaults to `16,384`)
- `LinkCacheSize` - The maximum number of entries links to chace (defaults to `1024`)

The exact storage usage depends on the size of multihashes. For example, using the default config to
advertise 128-bit long multihashes will result in chunk sizes of 0.25MiB with maximum cache growth
of 256 MiB.

To delete the cache set `PurgeLinkCache` to `true` and restart the engine.

Note that the LRU cache may grow beyond its max size if the generated chain of chunks is longer than
the configured `LinkChunkSize`. This is to avoid partial caching of chunks within a single
advertisement. The cache expansion is logged in `INFO` level at `provider/engine` logging subsystem.

## Related Resources

* [Indexer Ingestion Interface](https://www.notion.so/protocollabs/Indexer-Ingestion-Interface-4a120c698b31417385204ec401137cb1)
* [Indexer Ingestion IPLD Schema](https://github.com/filecoin-project/storetheindex/blob/main/api/v0/ingest/schema/schema.ipldsch)
* [Indexer Node Design](https://www.notion.so/protocollabs/Indexer-Node-Design-4fb94471b6be4352b6849dc9b9527825)
* [Providing data to a network indexer](https://github.com/filecoin-project/storetheindex/blob/main/doc/ingest.md)
* [`storetheindex`](https://github.com/filecoin-project/storetheindex): indexer node implementation
* [`storetheindex` documentation](https://github.com/filecoin-project/storetheindex/blob/main/doc/)
* [`go-indexer-core`](https://github.com/filecoin-project/go-indexer-core): Core index key-value
  store

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
