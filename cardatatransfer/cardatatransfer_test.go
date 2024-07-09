package cardatatransfer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	bstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync/storeutil"
	"github.com/ipfs/go-test/random"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/ipni/index-provider/cardatatransfer"
	"github.com/ipni/index-provider/supplier"
	"github.com/ipni/index-provider/testutil"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestCarDataTransfer(t *testing.T) {
	contextID1 := []byte("cheese")
	rdOnlyBS1 := testutil.OpenSampleCar(t, "sample-v1-2.car")

	roots1, err := rdOnlyBS1.Roots()
	require.NoError(t, err)
	require.Len(t, roots1, 1)

	contextID2 := []byte("applesauce")
	rdOnlyBS2 := testutil.OpenSampleCar(t, "sample-wrapped-v2-2.car")

	roots2, err := rdOnlyBS2.Roots()
	require.NoError(t, err)
	require.Len(t, roots2, 1)

	missingCid := random.Cids(1)[0]
	missingContextID := []byte("notFound")

	supplier := &fakeSupplier{blockstores: make(map[string]supplier.ClosableBlockstore)}
	supplier.blockstores[string(contextID1)] = rdOnlyBS1
	supplier.blockstores[string(contextID2)] = rdOnlyBS2

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	partialSelector := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Hash", ssb.Matcher())
		})))
	}).Node()

	partialBs, partialCount := copySelectorOutputToBlockstore(t, rdOnlyBS2, roots2[0], partialSelector, dagpb.Type.PBNode)
	require.Equal(t, partialCount, 2)

	pieceCID1 := pieceCIDFromContextID(t, contextID1)
	pieceCID2 := pieceCIDFromContextID(t, contextID2)
	missingPieceCID := pieceCIDFromContextID(t, missingContextID)

	incorrectPieceCid := random.Cids(1)[0]

	testCases := map[string]struct {
		voucher                  datatransfer.TypedVoucher
		root                     cid.Cid
		selector                 ipld.Node
		expectSuccess            bool
		expectMessage            string
		expectedBlockstoreResult bstore.Blockstore
	}{
		"select all": {
			voucher: (&retrievaltypes.DealProposal{
				PayloadCID: roots1[0],
				ID:         1,
				Params: retrievaltypes.Params{
					PieceCID: &pieceCID1,
				},
			}).AsVoucher(),
			root:                     roots1[0],
			selector:                 selectorparse.CommonSelector_ExploreAllRecursively,
			expectSuccess:            true,
			expectedBlockstoreResult: rdOnlyBS1,
		},
		"select partial": {
			voucher: (&retrievaltypes.DealProposal{
				PayloadCID: roots2[0],
				ID:         2,
				Params: retrievaltypes.Params{
					PieceCID: &pieceCID2,
					Selector: retrievaltypes.CborGenCompatibleNode{
						Node: partialSelector,
					},
				},
			}).AsVoucher(),
			root:                     roots2[0],
			selector:                 partialSelector,
			expectSuccess:            true,
			expectedBlockstoreResult: partialBs,
		},
		"no blockstore for context ID": {
			voucher: (&retrievaltypes.DealProposal{
				PayloadCID: missingCid,
				ID:         3,
				Params: retrievaltypes.Params{
					PieceCID: &missingPieceCID,
				},
			}).AsVoucher(),
			root:          missingCid,
			selector:      selectorparse.CommonSelector_ExploreAllRecursively,
			expectSuccess: false,
			expectMessage: "error reading blockstore: Not found!",
		},
		"piece cid that has no context id": {
			voucher: (&retrievaltypes.DealProposal{
				PayloadCID: roots1[0],
				ID:         4,
				Params: retrievaltypes.Params{
					PieceCID: &incorrectPieceCid,
				},
			}).AsVoucher(),
			root:          roots1[0],
			selector:      selectorparse.CommonSelector_ExploreAllRecursively,
			expectSuccess: false,
			expectMessage: "incorrect Piece CID codec",
		},
		"no piece cid": {
			voucher: (&retrievaltypes.DealProposal{
				PayloadCID: roots1[0],
				ID:         5,
			}).AsVoucher(),
			root:          roots1[0],
			selector:      selectorparse.CommonSelector_ExploreAllRecursively,
			expectSuccess: false,
			expectMessage: "must specific piece CID",
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			mn := mocknet.New()
			srcHost, err := mn.GenPeer()
			require.NoError(t, err)
			srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
			srcDt := testutil.SetupDataTransferOnHost(t, srcHost, srcStore, cidlink.DefaultLinkSystem())
			err = cardatatransfer.StartCarDataTransfer(srcDt, supplier)
			require.NoError(t, err)
			dstHost, err := mn.GenPeer()
			require.NoError(t, err)
			dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
			dstBlockstore := bstore.NewBlockstore(dstStore)
			lsys := storeutil.LinkSystemForBlockstore(dstBlockstore)
			dstDt := testutil.SetupDataTransferOnHost(t, dstHost, dstStore, lsys)
			err = mn.LinkAll()
			require.NoError(t, err)

			var expectedLen int
			// read blockstore length ahead of time
			if data.expectedBlockstoreResult != nil {
				expectedLen = testutil.GetBstoreLen(ctx, t, data.expectedBlockstoreResult)
			}
			dstResultChan := make(chan bool, 1)
			var dstMessage string
			dstDt.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.NewVoucherResult {
					vr, err := retrievaltypes.DealResponseFromNode(channelState.LastVoucherResult().Voucher)
					if err == nil {
						dstMessage = vr.Message
					}
				}
				if channelState.Status() == datatransfer.Cancelled || channelState.Status() == datatransfer.Failed {
					dstResultChan <- false
				}
				if channelState.Status() == datatransfer.Completed {
					dstResultChan <- true
				}
			})
			err = dstDt.RegisterVoucherType(retrievaltypes.DealProposalType, nil)
			require.NoError(t, err)
			_, err = dstDt.OpenPullDataChannel(ctx, srcHost.ID(), data.voucher, data.root, data.selector)
			require.NoError(t, err)

			select {
			case <-ctx.Done():
				require.FailNow(t, "context closed")
			case dstResult := <-dstResultChan:
				require.Equal(t, data.expectSuccess, dstResult)
				if data.expectSuccess {
					receivedLen := testutil.GetBstoreLen(ctx, t, dstBlockstore)
					require.Equal(t, expectedLen, receivedLen)
				} else {
					require.Equal(t, data.expectMessage, dstMessage)
				}
			}
		})
	}
}

type fakeSupplier struct {
	blockstores map[string]supplier.ClosableBlockstore
}

func (fs *fakeSupplier) ReadOnlyBlockstore(contextID []byte) (supplier.ClosableBlockstore, error) {
	bs, ok := fs.blockstores[string(contextID)]
	if !ok {
		return nil, errors.New("Not found!")
	}
	return bs, nil
}

func pieceCIDFromContextID(t *testing.T, contextID []byte) cid.Cid {
	md, err := cardatatransfer.TransportFromContextID(contextID)
	require.NoError(t, err)
	return md.(*metadata.GraphsyncFilecoinV1).PieceCID
}

func copySelectorOutputToBlockstore(t *testing.T, sourceBs bstore.Blockstore, root cid.Cid, selectorNode datamodel.Node, np datamodel.NodePrototype) (bstore.Blockstore, int) {
	bsOutput := bstore.NewBlockstore(datastore.NewMapDatastore())
	count := 0
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
		asCidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unsupported link type")
		}
		block, err := sourceBs.Get(lctx.Ctx, asCidLink.Cid)
		if err != nil {
			return nil, err
		}
		err = bsOutput.Put(lctx.Ctx, block)
		if err != nil {
			return nil, err
		}
		count++
		return bytes.NewBuffer(block.RawData()), nil
	}
	nd, err := lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, np)
	require.NoError(t, err)
	compiled, err := selector.CompileSelector(selectorNode)
	require.NoError(t, err)
	err = traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:        context.Background(),
			LinkSystem: lsys,
			LinkTargetNodePrototypeChooser: dagpb.AddSupportToChooser(func(datamodel.Link, linking.LinkContext) (datamodel.NodePrototype, error) {
				return basicnode.Prototype.Any, nil
			}),
		},
	}.WalkAdv(nd, compiled, func(traversal.Progress, datamodel.Node, traversal.VisitReason) error { return nil })
	require.NoError(t, err)

	return bsOutput, count
}
