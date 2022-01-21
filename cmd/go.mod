module github.com/filecoin-project/index-provider/cmd

go 1.16

require (
	github.com/filecoin-project/go-data-transfer v1.13.0
	github.com/filecoin-project/go-legs v0.2.3
	github.com/filecoin-project/index-provider v0.2.1
	github.com/filecoin-project/storetheindex v0.2.3
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-graphsync v0.12.0
	github.com/ipfs/go-ipfs v0.11.0
	github.com/ipfs/go-log/v2 v2.5.0
	github.com/ipld/go-car/v2 v2.1.1
	github.com/ipld/go-ipld-prime v0.14.4
	github.com/libp2p/go-libp2p v0.17.0
	github.com/libp2p/go-libp2p-core v0.13.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.5.0
	github.com/multiformats/go-multicodec v0.4.0
	github.com/multiformats/go-multihash v0.1.0
	github.com/rogpeppe/go-internal v1.8.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/index-provider => ../
