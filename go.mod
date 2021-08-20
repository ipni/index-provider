module github.com/filecoin-project/indexer-reference-provider

go 1.16

require (
	github.com/filecoin-project/go-indexer-core v0.0.0-20210816132949-bbccdebb905f
	github.com/filecoin-project/storetheindex v0.0.0-20210817090158-08baf74302ee
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.4.6
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/ipld/go-ipld-prime v0.11.1-0.20210819131917-d7e93a828c7c
	github.com/lib/pq v1.10.2
	github.com/libp2p/go-libp2p-core v0.8.6
	github.com/mitchellh/go-homedir v1.1.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/willscott/go-legs v0.0.0-20210818075656-0135c1be9fd6
)

// TODO: Remove when https://github.com/filecoin-project/storetheindex/pull/42 is merged.
replace github.com/filecoin-project/storetheindex => ../storetheindex
