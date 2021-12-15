module github.com/filecoin-project/index-provider/cmd

go 1.16

require (
	github.com/aws/aws-sdk-go-v2/service/sqs v1.13.1 // indirect
	github.com/filecoin-project/go-data-transfer v1.11.4
	github.com/filecoin-project/index-provider v0.0.0-20211115210313-7957526f5b07
	github.com/filecoin-project/storetheindex v0.0.0-20211201165140-0676bdcf0a0b
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipfs/go-graphsync v0.10.4
	github.com/ipfs/go-ipfs v0.10.0
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/ipld/go-car/v2 v2.0.3-0.20210920144420-f35d88ce16ca
	github.com/ipld/go-ipld-prime v0.14.0
	github.com/libp2p/go-libp2p v0.15.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.4.1
	github.com/multiformats/go-multicodec v0.3.1-0.20210902112759-1539a079fd61
	github.com/multiformats/go-multihash v0.1.0
	github.com/rogpeppe/go-internal v1.8.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/index-provider => ../
