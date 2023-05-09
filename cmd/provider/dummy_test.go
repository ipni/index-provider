package main

import (
	"context"
	"fmt"
	"testing"

	b64 "encoding/base64"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestT(t *testing.T) {
	// context id that we would like to find among advertisements
	ctxID := "AXESIIgKWX3EESkqmj/rnKOoUUM7B8UClNEYV1bkAv81YihL"
	//the last known ad cid
	adCid := "baguqeerasutetanro64rn2ovs6n2uejjk67ffs4fwg2btcuazghtgz6z2jwq"
	// the address of the provider
	providerAddr := "/ip4/76.219.232.45/tcp/24001/p2p/12D3KooWPNbkEgjdBNeaCGpsgCrPRETe4uBZf1ShFXStobdN18ys"

	ctx := context.Background()

	adc, err := cid.Decode(adCid)
	require.NoError(t, err)

	pclient, err := toProviderClient(providerAddr, "/indexer/ingest/mainnet")
	require.NoError(t, err)

	for {
		fmt.Printf("\nFetching %s", adc.String())
		ad, err := pclient.GetAdvertisement(ctx, adc)
		require.NoError(t, err)
		if !ad.IsRemove {
			b64CtxID := b64.StdEncoding.EncodeToString(ad.ContextID)
			if b64CtxID == ctxID {
				fmt.Printf("\nFound %s", adc.String())
				return
			}
		} else {
			fmt.Printf("\nAd %s is for removal", adc.String())
		}
		adc = ad.PreviousID
		if adc == cid.Undef {
			fmt.Printf("\nDone")
			return
		}
	}
}
