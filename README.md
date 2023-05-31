Index Provider :loudspeaker:
============================
[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![Go Reference](https://pkg.go.dev/badge/github.com/ipni/index-provider.svg)](https://pkg.go.dev/github.com/ipni/index-provider)
[![Coverage Status](https://codecov.io/gh/ipni/index-provider/branch/main/graph/badge.svg)](https://codecov.io/gh/ipni/index-provider/branch/main)

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
      using [`go-libipni/announce`](https://pkg.go.dev/github.com/ipni/go-libipni/announce)
    * `MultihashLister` integration point for fully customizable look up of advertised multihashes.
    * Utilities to advertise multihashes directly [from CAR files](supplier/car_supplier.go)
      or [detached CARv2 index](index_mh_iter.go) files.
    * Index advertisement [`go-libipni/metadata`](https://pkg.go.dev/github.com/ipni/go-libipni/metadata) schema for retrieval
      over [graphsync](https://pkg.go.dev/github.com/ipni/go-libipni/metadata#GraphsyncFilecoinV1) and [bitswap](https://pkg.go.dev/github.com/ipni/go-libipni/metadata#Bitswap)

## Current status :construction:

This implementation is under active development.

## Background

The protocol implemented by this repository is the index provider portion of a larger indexing protocol documented [here](https://www.notion.so/protocollabs/Indexer-Node-Design-4fb94471b6be4352b6849dc9b9527825)
. The indexer node implementation can be found at [`storetheindex`](https://github.com/ipni/storetheindex) and [`go-libipni`](https://github.com/ipni/go-libipni).

For more details on the ingestion protocol itself
see [Providing data to a network indexer](https://github.com/ipni/storetheindex/blob/main/doc/ingest.md)
.

## Install

Prerequisite:

- [Go 1.19+](https://golang.org/doc/install)

To use the provider as a Go library, execute:

```shell
go get github.com/ipni/index-provider
```

To install the latest `provider` CLI, run:
<!-- 
Note: installation instructions uses `git clone` because the `cmd` module uses `replace` directive 
and cannot be installed directly via `go install`
-->

```shell
go install github.com/ipni/index-provider/cmd/provider@latest
```

Alternatively, download the executables directly from
the [releases](https://github.com/ipni/index-provider/releases).

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

#### Exposing delegated routing server from provider (experimental)

Provider can export a Delegated Routing server. Delegated Routing allows IPFS nodes to advertise their contents to indexers alongside DHT. 
Delegated Routing server is off by default. To enable it, add the following configuration block to the provider config file.

```
{
  ...
  DelegatedRouting {
    ListenMultiaddr: "/ip4/0.0.0.0/tcp/50617 (example)"
  }
  ...
}
```

#### Configuring Kubo and index-provider to use IPNI

Kubo supports HTTP delegated routing as of v0.20.0. The following section contains configuration examples and a few tips to enable Kubo to advertise its CIDs to 
IPNI systems like `cid.contact` using `index-provider`. Delegated Routing is still in the Experimental stage and configuration might change from version to version. 
This section is not Alma Mater for all-things Kubo. It rather exists to give one inspiration of how to configure their node to use IPNI. 
Please refer to the [Kubo docs](https://docs.ipfs.tech/install/command-line/) for more thorough info. A few things to keep in mind:

* `index-provider`'s delegated routing server should be run at all times as a "sidecar" to the Kubo node. `index-provider` can be safely restarted, however
if it's down, no new CIDs will be flowing from Kubo to IPNI;
* `index-provider` doesn't support Reframe anymore. Please upgrade to Kubo v0.20.+ with HTTP Delegated Routing support;
* Kubo advertises it's data in _snapshots_. That means that all CIDs under its management get _reprovided_ into the configured routers every 12/24 hours (configurable).
That mechanism takes its roots from the way DHT works. `index-provider` can track what CIDs have been removed / added in between two consecuitive snapshots and convert
that information into IPNI advertisements. Depending on the size of the node, during reproviding process one might see a lot of chatter between all processes involved.
In between reprovides, Kubo still sends new individual CIDs to the configured routers;
* Kubo needs `index-provider` only to publish its CIDs to IPNI. Kubo can do IPNI lookups natively without a sidecar involved (see the Kubo docs on `auto` routers);
* `index-provider` must be reachable from the Internet. IPNI will try to establish conneciton into it to fetch Advertisement chains. If that can't be done - CIDs will not appear in IPNI. 
It's important to configure your firewall accordingly. Take a note of the `ProviderServer` port from the `index-provider` configuration and open it up on the firewall.

Configure `index-provider` to expose delegated rouring server:
```
"DelegatedRouting": {
  "ListenMultiaddr": "/ip4/0.0.0.0/tcp/50617",
  "ProviderID": "PEER ID OF YOUR IPFS NODE",
  "Addrs": // List of multiaddresses that you'd like to be advertised to IPNI. If not specified, Swarm addresses of the Kubo node will be used.
}
```

Configure Kubo to publish into both DHT and IPNI:
```
"Routing": {
    "Methods": {
      "find-peers": {
        "RouterName": "WanDHT"
      },
      "find-providers": {
        "RouterName": "ParallelHelper"
      },
      "get-ipns": {
        "RouterName": "WanDHT"
      },
      "provide": {
        "RouterName": "ParallelHelper"
      },
      "put-ipns": {
        "RouterName": "WanDHT"
      }
    },
    "Routers": {
      "IndexProvider": {
        "Parameters": {
          "Endpoint": "http://127.0.0.1:50617",
          "MaxProvideBatchSize": 10000,
          "MaxProvideConcurrency": 1
        },
        "Type": "http"
      },
      "ParallelHelper": {
        "Parameters": {
          "Routers": [
            {
              "IgnoreErrors": true,
              "RouterName": "IndexProvider",
              "Timeout": "30m"
            },
            {
              "IgnoreErrors": true,
              "RouterName": "WanDHT",
              "Timeout": "30m"
            }
          ]
        },
        "Type": "parallel"
      },
      "WanDHT": {
        "Parameters": {
          "AcceleratedDHTClient": false,
          "Mode": "auto",
          "PublicIPNetwork": true
        },
        "Type": "dht"
      }
    },
    "Type": "custom"
  },
```

With the configuration above, Kubo will advertise its CIDs to both DHT and IPNI and will also use both DHT and IPNI for `find-providers` lookups.
Additionally enable the following flag in the Kubo config to turn on batch reprovides on (especially for larger nodes). 
 ```
"Experimental": { 
  "AcceleratedDHTClient": true,
},
```

Try adding a new file to your Kubo node and you should see `index-provider` logs start rolling instantly. If that doesn't happen, then most likely Kubo has been configured incorrectly.

`index-provider` publishes announcements about new advertisements on a libp2p pub/sub topic. This topic is listened by IPNI systems like `cid.contact`. Once a new announcement is seen, 
IPNI would reach out to `index-provider` to download advertisement chains and index the content. It's important to keep in mind:
* There might be a delay before IPNI picks up an announcement from the libp2p pub/sub depending on the network, number of hops and etc;
* There might be a delay before IPNI reaches out to `index-provider` depending on the overall business of the system;
* If no comminication has been received from IPNI within a reasonable amount of time then most likely `index-provider` is not reachable from the Internet. You can verify whether
it's reachable by using `index-provider` CLI. For example `provider ls ad --provider-addr-info=/ip4/76.21.23.45/tcp/24001/p2p/12D3KooWPNbEgjdBNeaCGpsgCrPRETe4uBZf1ShFXSdN18ys` (replace with the correct 
multiaddress and peer id of your `index-provider`). Remember to run this command not from the same computer where `index-provider` is. 

Here are a few additional configuration options to consider:

* `ChunkSize`: `index-provider` publishes advertisements with a certain number of CIDs in each chunk. An advertisement needs to accumulate enough CIDs before it gets published. You can reduce the `ChunkSize` parameter to publish data more quickly. The default value is 1000.
* `AdFlushFrequency` - `index-provider` can publish Advertisements before they get full. That is driven by `AdFlushFrequency`. In other words, an advertisement will be published
either when it has reached `ChunkSize` or after `AdFlushFrequency`. Set it to lower values in order to get the data out quicker. The default is 10m.

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

[Extended providers](https://github.com/ipni/storetheindex/blob/main/doc/ingest.md#extendedprovider) 
field allows for specification of provider families, in cases where a provider operates multiple PeerIDs, perhaps 
with different transport protocols between them, but over the same database of content. 

`ExtendedProviders` can either be applied at the *chain-level* (for all previous and future CIDs published by a provider) or at 
a *context-level* (for CIDs belonging to the specified context ID). That behaviour is set by `ContextID` field.
Multiple different behaviours can be triggered by a combination of `ContextID`, `Metadata`, `ExtendedProviders` and `Override` fields. 
For more information see the [specification](https://github.com/ipni/storetheindex/blob/main/doc/ingest.md#extendedprovider) 

Advertisements with `ExtendedProviders` can be composed manually or by using a convenience `ExtendedProvidersAdBuilder` 
and will have to be signed by the main provider as well as by all `ExtendedProviders`' identities. 
Private keys for these identities have to be provided in the `xproviders.Info` 
(objects)[https://github.com/ipni/index-provider/blob/main/engine/xproviders/xproviders.go] and  
`ExtendedProvidersAdBuilder` will take care of the rest.

> Identity of the main provider will be added to the extended providers list automatically and should not be passed in explicitly. 

Some examples can be found below (assumes the readers familiriaty with the 
(specification)[https://github.com/ipni/storetheindex/blob/main/doc/ingest.md#extendedprovider]).

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
(here)[https://github.com/ipni/index-provider/blob/main/engine/xproviders/xproviders_test.go].

### `provider` CLI

The `provider` CLI can be used to interact with a running daemon via the admin server to perform a
range of administrative operations. For example, the `provider` CLI can be used to import a CAR file
and advertise its content to the indexer nodes by executing:

```shell
provider import car -l http://localhost:3102 -i <path-to-car-file>
```

For usage description, execute `provider --help`

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
in [`Ingest`](https://pkg.go.dev/github.com/ipni/index-provider@v0.2.6/config#Ingest)
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

* [Indexer Ingestion IPLD Schema](https://github.com/ipni/go-libipni/blob/main/ingest/schema/schema.ipldsch)
* [Indexer Node Design](https://www.notion.so/protocollabs/Indexer-Node-Design-4fb94471b6be4352b6849dc9b9527825)
* [Providing data to a network indexer](https://github.com/ipni/storetheindex/blob/main/doc/ingest.md)
* [`storetheindex`](https://github.com/ipni/storetheindex): indexer node implementation
* [`storetheindex` documentation](https://github.com/ipni/storetheindex/blob/main/doc/)
* [`go-indexer-core`](https://github.com/filecoin-project/go-indexer-core): Core index key-value store

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
