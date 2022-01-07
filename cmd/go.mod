module github.com/filecoin-project/index-provider/cmd

go 1.16

require (
	github.com/filecoin-project/go-data-transfer v1.12.1
	github.com/filecoin-project/go-ds-versioning v0.1.1 // indirect
	github.com/filecoin-project/index-provider v0.0.0-20211115210313-7957526f5b07
	github.com/filecoin-project/storetheindex v0.2.0
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-graphsync v0.11.5
	github.com/ipfs/go-ipfs v0.10.0
	github.com/ipfs/go-log/v2 v2.4.0
	github.com/ipld/go-car/v2 v2.1.1
	github.com/ipld/go-ipld-prime v0.14.3
	github.com/libp2p/go-libp2p v0.17.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.5.0
	github.com/multiformats/go-multicodec v0.4.0
	github.com/multiformats/go-multihash v0.1.0
	github.com/rogpeppe/go-internal v1.8.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/index-provider => ../
