# Publisher Configuration

## Modes of Operation

The content advertisement publisher is able to make content advertisements retrievable using four modes of operation:

- HTTP served over libp2p (default)
- Plain HTTP using publisher server
- Plain HTTP using external server
- Data-transfer/graphsync (will be discontinued)

Each of these modes of operation is enabled using a set of engine options, that can be specified via the engine API, or specified in a configuration file when using the command-line.

### HTTP or DataTransfer Publisher

The index-provider engine must be configured to use either an HTTP publisher or a DataTransfer publisher. It cannot use both. The first three modes of operation use an HTTP publisher and the last uses a DataTransfer publisher. To specify whether to use an HTTP or DataTransfer publisher, use the `WithPublisherKind` option:

`WithPublisherKind` sets the kind of publisher used to announce new advertisements. If unset, advertisements are only stored locally and no announcements are made.

```go
// HttpPublisher exposes an HTTP server that allows peers in the network to
// sync advertisements over a raw HTTP transport.
WithPublisherKind(HttpPublisher)

// DataTransferPublisher exposes a datatransfer/graphsync server that
// allows peers in the network to sync advertisements.
//
// This option is being discontinued. Only provided as a fallback in case
// HttpPublisher is not working.
WithPublisherKind(DataTransferPublisher)

// NoPublisher indicates that no announcements are made to the network and
// all advertisements are only stored locally.
WithPublisherKind(NoPublisher)
```

If configuring the command-line application, this is configured by setting the `config.Ingest.PublisherKind` item in the configuration file to a value of "http", "dtsync", or "".

In future index-provider releases support for the DataTransfer publisher will be removed.

## Publishing vs Announcing

When a new advertisement is made available by a publisher, the new advertisement's CID is generally announced to one or more indexers. An announcement message is constructed and is sent to indexers over libp2p gossip pubsub where it is received by any indexers subscribed to the topic on which the message was publisher. Or, the announcement message is send directly to specific indexers by HTTP.

One, both, or none of these announcement methods may be used in conjunction with the publisher, without regard to the whether the publisher is configured as one of the HTTP variations or as a DataTransfer publisher. It is only necessary that the announcement message is create to contain one or more addresses that are usable by indexers to contact the publisher. Otherwise, the configuration of announcement senders is totally separate from the publisher, and announcements will not be discussed any more in this document.

## Configuration of HTTP Publisher

An HTTP publisher has three variations: libp2p, publisher HTTP server, external HTTP server. Here is how these modes are selected.

- **libp2p**: The libp2p variation is the default and uses the engine's libp2p stream host. This can be disabled by providing true to the `WithHttpNoLibp2p` option.
- **Publisher-served Plain HTTP**: This is enabled when an HTTP listen address is supplied by the `WithHttpPublisherListenAddr` option. The corresponding config file item is `config.Ingest.HttpPublisher.ListenMultiaddr`.
- **External HTTP Server**: This is selected by using the `WithHttpPublisherWithoutServer` option. This also ignores the previous two options. There is no config file item for this as it is only available when an application using the engine API supplies an HTTP server.

### HTTP served over libp2p

This is the default mode of operation, and allows HTTP requests and responses to be communicated over libp2p. A libp2p stream host must be provided to use this mode of operation.

#### libp2p Stream Host

The engine always has a libp2p stream host supplied to it with the `WithHost` option or created internally. This libp2p host is give to the engine's HTTP publisher. The private key associated with the libp2p host's Identity is given to the engine using the `WithPrivateKey` option, and is also given to the publisher. This allows advertisements to be signed.

If using the command-line, the libp2p host Identity and private key are configured using the `config.Identiry.PeerID` and `config.Identity.PrivKey` configuration file items. The libp2p host's listen address is configured using the config file item `config.ProviderServer.ListenMultiaddr`.

When serving HTTP over libp2p, it is not necessary to specify a publisher HTTP listen address since the libp2p stream host is responsible for handling connections. If a publisher HTTP listen address is specified, and libp2p publisher in no disabled by `WithHttpNoLibp2p(true)`, then the publisher will serve advertisements on **_both_** HTTP over libp2p and plain HTTP.

### Publisher-served Plain HTTP

The publisher can be configured to server HTTP using a plain (non-libp2p) HTTP server. This is enabled by default, but can be chosen exclusively by disabling libp2p publishing using the engine option `WithHttpNoLibp2p(true)`. The HTTP listen address is supplied using the engine option:
```go
WithHttpPublisherListenAddr
```
`WithHttpPublisherListenAddr` sets the net listen address for the HTTP publisher. If unset, the default net listen address of '0.0.0.0:3104' is used.

To disable publisher-served plain HTTP, specify no HTTP listen address with `WithHttpPublisherListenAddr("")`

### External HTTP Server

To publish content advertisements over HTTP using a server other than the one provided by the publisher, disable starting the publisher server using the engine option:
```go
WithHttpPublisherWithoutServer
```
`WithHttpPublisherWithoutServer` WithHttpPublisherWithoutServer directs the publisher to not start its HTTP server.

The engine function `GetPublisherHttpFunc()` returns the publisher's `ServeHTTP` function to use as a handler, with an external HTTP server, for handling advertisement and chain head requests. The publisher's HTTP handler expects the request URL path to start with the IPNI path `/ipni/v1/ad/`. If this is not present, then handler will not handle the request.

### HTTP Request URL Path

The publisher expects any HTTP request URL to contain the IPNI path `/ipni/v1/ad/`. Following the IPNI path is the requested resource, which is either the value `head` to request information about the advertisement chain head, an advertisement CID string to request an advertisement, or an advertisement multihash entry block CID to request multihash data.

There is no need to specify the IPNI path anywhere, and the publisher always expects it to be in any incoming request URL that is to be handled by the publisher. Likewise, there is no need to specify this in announcement messages, or to indexers in any way. Indexers will implicitly add this path to any URL making requests to an IPNI publisher.

The IPNI path in the URL may optionally be preceded by a user-defined path. This can be specified by the engine option:
```go
WithHttpPublisherHandlerPath
```
This may be useful when using an external server that serves the IPNI publisher under some existing path. If a handler path of "/foo/bar/" is specified, then the URL path for a chain head query will be "/foo/bar/ipni/v1/ad/head". The publisher will only handle requests that have a URL path starting with "/foo/bar/ipni/v1/ad/".

## DataTransfer Publisher Configuration

Publishing with data-transfer/graphsync is legacy configuration, and is only supported by the IPNI publisher for now as a fall-back in case there is some unforeseen problem with the new HTTP over libp2p. Publisher support for this will be dropped is future releases of the index-provider.

A data-transfer publisher is configured by specifying the engine option `WithPublisherKind(DataTransferPublisher)`. When this option is specified, all of the HTTP publisher options are ignored.
