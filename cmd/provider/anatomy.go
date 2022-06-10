package main

import (
	"encoding/hex"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"strings"
)

var anatomyFlags = []cli.Flag{
	&cli.StringFlag{
		Name:"cid",
		Usage: "specified cid",
		Required: true,
	},
}

var AnatomyCmd = &cli.Command{
	Name: "anatomy",
	Usage: "the anatomy of cid",
	Flags: anatomyFlags,
	Action: anatomyCommand,
}

var (
	mb string
	ver uint64
	codec string
	mh string
)

// attempt to offer an alternative for cid inspector due to instability

// bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
// https://proto.school/tutorial-assets/T0006L06-example-1.png
func anatomyCommand(cctx *cli.Context) error {
	cidStr := cctx.String("cid")

	mbEnc,_,err := multibase.Decode(cidStr)
	mb = multibase.EncodingToStr[mbEnc]

	c,err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("%s cid-decoding error:%v",cidStr,err)
	}

	dmh,err := multihash.Decode(c.Hash())
	if err != nil {
		return fmt.Errorf("%s multihash-decoding error:%v",cidStr,err)
	}
	//fmt.Println(dmh.Name,dmh.Length,dmh.Digest)

	/*
	prefix
		00000001 01110000       00010010   00100000            hashed bytes
		cidv     codec:protobuf sha2-256   hash len::32 bytes  ...
	multihash
		00010010   00100000            hashed bytes
		sha2-256   hash len::32 bytes  ...
	*/
	prefix := c.Prefix()

	//ver = prefix.Version
	ver = c.Version()
	codec = cid.CodecToStr[prefix.Codec]
	//mh = multihash.Codes[prefix.MhType]
	mh = dmh.Name
	mhl := dmh.Length * 8
	//fmt.Println(prefix.Bytes())
	hx := strings.ToUpper(hex.EncodeToString(dmh.Digest))

	fmt.Println("HUMAN READABLE CID")
	fmt.Printf("  %s - cidv%d - %s - (%s : %d : %s)\n",mb,ver,codec,mh,mhl,hx)
	//fmt.Printf("%s - cidv%d - %s - (%s : %d : %s)\n",mb,ver,codec,mh,mhl,c.Hash().B58String())
	//fmt.Printf("%s - cidv%d - %s - (%s : %d : %s)\n",mb,ver,codec,mh,mhl,c.Hash().HexString())
	fmt.Println("  MULTIBASE - VERSION - MULTICODEC - MULTIHASH(NAME:SIZE:DIGEST IN HEX)")
	//fmt.Printf("%c - %d - Ox%x - (Ox%x : %d : %s)\n",mbEnc,ver,prefix.Codec,prefix.MhType,mhl,c.Hash().String())

	fmt.Println("\n")

	fmt.Println("MULTIBASE")
	fmt.Println("PREFIX:")
	fmt.Printf("  %c\n", mbEnc)
	fmt.Println("NAME:")
	fmt.Printf("  %s\n", mb)

	fmt.Println("\n")

	fmt.Println("MULTICODEC")
	fmt.Println("CODE:")
	fmt.Printf("  Ox%x\n", prefix.Codec)
	fmt.Println("NAME:")
	fmt.Printf("  %s\n", codec)

	fmt.Println("\n")

	fmt.Println("MULTIHASH")
	fmt.Println("CODE:")
	fmt.Printf("  Ox%x\n", prefix.MhType)
	fmt.Println("NAME:")
	fmt.Printf("  %s\n", mh)
	fmt.Println("BITS")
	fmt.Println("  ",mhl)
	fmt.Println("DIGEST (HEX):")
	fmt.Printf("  %s\n",hx)

	fmt.Println("\n")

	fmt.Println("MH FOR INDEX")
	fmt.Println("  ",c.Hash().B58String())

	fmt.Println("\n")

	if ver == 0 {
		fmt.Println("CIDV1")
		fmt.Println("  ",cid.NewCidV1(multibase.Base58BTC,c.Hash()).String())
	}

	//s := "C3C4733EC8AFFD06CF9E9FF50FFC6BCD2EC85A6170004BB709669C31DE94391A"
	//bytes,err := hex.DecodeString(s)
	//fmt.Println(bytes)
	//fmt.Println([]byte(c.Hash()))

	return nil
}