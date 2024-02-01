# Publisher Configuration

## Modes of Operation

The content advertisement publisher is able to make content advertisements retrievable using five modes of operation:

- HTTP served over libp2p (default)
- Plain HTTP using publisher's server
- Plain HTTP using external server
- HTTP served over libp2p and Plain HTTP together
- Data-transfer/graphsync (will be discontinued)

Each of these modes of operation is enabled using a set of engine options, that can be specified via the engine API, or specified in a configuration file when using the command-line.

### HTTP, Libp2p, Libp2p + HTTP, or DataTransfer Publisher

The index-provider engine must be configured to use a libp2p publisher, HTTP publisher, HTTP + libp2p combined publisher, or a DataTransfer publisher. Specify what kind of publisher to use by calling the `WithPublisherKind` engine option and passing it one of the following values:
- `NoPublisher` indicates that no announcements are made to the network and all advertisements are only stored locally.
- `Libp2pPublisher` serves advertisements using the engine's libp2p host.
- `HttpPublisher` exposes an HTTP server that serves advertisements using an HTTP server.
- `Libp2pHttpPublisher` serves advertisements using both HTTP and libp2p servers.
- `DataTransferPublisher` exposes a data-transfer/graphsync server that allows peers in the network to sync advertisements. This option is being discontinued. Only provided as a fallback in case HttpPublisher and Libp2pHttpPublisher are not working.

If `WithPublisherKind` is not provided a value, it defaults to `NoPublisher` and advertisements are only stored locally and no announcements are made. If configuring the command-line application, `WithPublisherKind` is configured by setting the `Ingest.PublisherKind` item in the configuration file to a value of "http", "libp2p", "libp2phttp, or "".

For all publisher kinds, except the `DataTransfer` publisher, the `WithHttpPublisherAnnounceAddr` option sets the addresses that are announced to indexers, telling the indexers where to fetch advertisements from. If configuring the command-line application, `WithHttpPublisherAnnounceAddr` is configured by specifying multiaddr strings in `Ingest.HttpPublisher.AnnounceMultiaddr`.

In future index-provider releases support for the DataTransfer publisher kind will be removed.

## Publishing vs Announcing

When a new advertisement is made available by a publisher, the new advertisement's CID is generally announced to one or more indexers. An announcement message is constructed and is sent to indexers over libp2p gossip pubsub where it is received by any indexers subscribed to the topic on which the message was publisher. The announcement message may also be sent directly to specific indexers via HTTP.

The `WithDirectAnnounce` option enables sending announcements directly, via HTTP, to the indexer URLs specified. If this option is not configured or no URLs specified, then direct HTTP announcement is disabled. The corresponding config file item is `DirectAnnounce.URLs`. The `WithPubsubAnnounce` option configures whether or not to broadcast announcements to all subscribed indexers. The corresponding config file item is `DirectAnnounce.NoPubsubAnnounce`.

One, both, or none of these announcement methods may be used to make announcements for the publisher, without regard to what kind of publisher is configured. It is only necessary that the announcement message is created with one or more addresses that indexers can use to contact the publisher. The address(es) configured by `WithHttpPublisherAnnounceAddr` may depend on the type of publisher. Otherwise, the configuration of announcement senders is totally separate from the publisher.

If using a plain HTTP server, then provide addresses that specify the "http" or "https" protocol. For example "/dns4/ipni.example.com/tcp/80/https" uses "ipni.example.com" as a DNS address that is expected to resolve to a public address that handles TLS termination, as indicated by the "https" portion. If using a Libp2p server, then specify address(es) that your libp2p host can be contacted on _without_ the "http". Specifying multiple addresses to announce is OK. If no addresses are specified, then the listening HTTP addresses are used if there is an HTTP publisher, and the libp2p host addresses are used if there is a libp2p server.

## Configure HTTP server `HttpPublisher` publisher kind

The publisher can be configured to server HTTP using a plain (non-libp2p) HTTP server. This is configured by calling `WithPublisherKind(HttpPublisher)`. If configuring the command-line application, this is set in the configuration file as `Ingest.HttpPublisher.ListenMultiaddr`.

The publisher's HTTP listen address is configured using the engine option `WithHttpPublisherListenAddr`. If unset, the default net listen address of '0.0.0.0:3104' is used.

The HTTP listen addresses only supports http, and https is not currently supported. A future release may support https, but this should generally not be necessary since the advertisement data transferred is signed and immutable, and if TLS is required then it can be terminated outside the index-provider.

### Use HTTP server or Existing HTTP server

The `HttpPublisher` kind can use either the publisher's HTTP server, or use an existing external HTTP server. The publisher's HTTP server is used by default.

#### Publisher's HTTP server

This is the default for the `HttpPublisher`.

At least one HTTP listen address is required for the HTTP server to listen on. An HTTP listen address is supplied by the `WithHttpPublisherListenAddr` option. The corresponding config file item is `Ingest.HttpPublisher.ListenMultiaddr`. If left unspecified a default listen address is provided. This does not apply if using one of the http provider kinds.

#### Existing HTTP server

To avoid starting the publisher's HTTP server, call the `WithHttpPublisherWithoutServer` option passing it `true`. Use this when the publisher it to be used as an http handler with an existing server. There is no config file item for this as it is only available when an application, that supplies an HTTP server, is using the engine API.

The engine function `GetPublisherHttpFunc()` returns the publisher's `ServeHTTP` function to use as a handler, with an external HTTP server, for handling advertisement and chain head requests. The publisher's HTTP handler expects the request URL path to start with the IPNI path `/ipni/v1/ad/`. If this is not present, then handler will not handle the request.

### HTTP Request URL Path

The publisher expects any HTTP request URL to contain the IPNI path `/ipni/v1/ad/`. Following the IPNI path is the requested resource, which is either the value `head` to request information about the advertisement chain head, an advertisement CID string to request an advertisement, or an advertisement multihash entry block CID to request multihash data.

There is no need to specify the IPNI path anywhere, and the publisher always expects it to be in any incoming request URL that is to be handled by the publisher. Likewise, there is no need to specify this in announcement messages, or to indexers in any way. Indexers will implicitly add this path to any URL making requests to an IPNI publisher.

The IPNI path in the URL may optionally be preceded by a user-defined path. This can be specified by the engine option `WithHttpPublisherHandlerPath`. It is useful when using an external server that serves the IPNI publisher under some existing path. If a handler path of "/foo/bar/" is specified, then the URL path for a chain head query will be "/foo/bar/ipni/v1/ad/head". The publisher will only handle requests that have a URL path starting with "/foo/bar/ipni/v1/ad/". When using an existing HTTP server, the publisher's handler function expect the full handler path and will return an error if it is asked to handle a request with a URL that does not contain the full path.

## Configure HTTP over libp2p with `Libp2pPublisher` publisher kind

HTTP over libp2p is the default mode of operation. It allows HTTP requests and responses to be communicated over libp2p. A libp2p stream host must be provided to use this mode of operation.

When serving HTTP over libp2p, it is not necessary to specify a publisher HTTP listen address since the libp2p stream host is responsible for handling connections. Any publisher HTTP listen address is ignored. Announcement addresses, specified with `WithHttpPublisherAnnounceAddr`, do not need to include a `/http` component.

### libp2p Stream Host

The engine always has a libp2p stream host supplied to it with the `WithHost` option or created internally. This libp2p host is give to the engine's HTTP publisher. The private key associated with the libp2p host's Identity is given to the engine using the `WithPrivateKey` option, and is also given to the publisher. This allows advertisements to be signed.

If using the command-line, the libp2p host Identity and private key are configured using the `Identiry.PeerID` and `Identity.PrivKey` configuration file items. The libp2p host's listen address is configured using the config file item `ProviderServer.ListenMultiaddr`.

## Configure HTTP over libp2p and HTTP with `Libp2pHttpPublisher` publisher kind

This is a combination of the `Libp2pPublisher` and `HttpPublisher` kinds, and the configurations for both apply. The transport used depends on which address a sync client connects to the publisher on. This configuration may be useful for using different protocols on different networks, or offering a choice of protocols to indexers. The sync client (indexer) will determine which protocol is used by sending an initial probe to the publisher address is it connecting to. 

## DataTransfer Publisher Configuration

Publishing with data-transfer/graphsync is legacy configuration, and is only supported by the IPNI publisher for now as a fall-back in case there is some unforeseen problem with the new HTTP over libp2p. Publisher support for this will be dropped is future releases of the index-provider.

## Config Quick Reference

- Tell indexers where to fetch advertisements from:
  - `WithHttpPublisherAnnounceAddr`
  - `"Ingest"."HttpPublisher"."AnnounceMultiaddr"`
- Listen for plain HTTP requests for advertisements
  - `WithHttpPublisherListenAddr`
  - `"Ingest"."HttpPublisher"."ListenMultiaddr"`
- Tell retrieval clients where to retrieve content from, by advertising these addrs:
  - `WithRetrievalAddrs`
  - `"ProviderServer"."RetrievalMultiaddrs"`
- Configure the interface that the content-retrieval-server listens on and provider ID:
  - `WithProvider`
  - `"ProviderServer"."ListenMultiaddr"
- Send advertisement announcements to specific indexers via HTTP:
  - `WithDirectAnnounce`
  - `"DirectAnnounce"."URLs"`
- Disable/enable sending advertisement announcements via pubsub:
 - `WithPubsubAnnounce`
 - `"DirectAnnounce"."NoPubsubAnnounce"`


A data-transfer publisher is configured by specifying the engine option `WithPublisherKind(DataTransferPublisher)`. When this option is specified, all of the `HttpPublisher` and `Libp2pPublisher` options are ignored. This option is deprecated and will not be supported in the future.
