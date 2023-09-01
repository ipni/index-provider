package mirror_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/mirror"
	"github.com/ipni/index-provider/testutil"
	"github.com/libp2p/go-libp2p"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	sourceHost host.Host
	source     *engine.Engine
	sourceMhs  map[string][]multihash.Multihash

	mirror            *mirror.Mirror
	mirrorHost        host.Host
	mirrorSync        *ipnisync.Sync
	mirrorSyncLs      ipld.LinkSystem
	mirrorSyncer      *ipnisync.Syncer
	mirrorSyncLsStore *memstore.Store
}

func (te *testEnv) startMirror(t *testing.T, ctx context.Context, opts ...mirror.Option) {
	privKey, _, err := p2pcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	te.mirrorHost, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.Identity(privKey))
	require.NoError(t, err)
	// Override the host, since test environment needs explicit access to it.
	opts = append(opts, mirror.WithHost(te.mirrorHost, privKey))
	te.mirror, err = mirror.New(ctx, te.sourceAddrInfo(t), opts...)
	require.NoError(t, err)
	require.NoError(t, te.mirror.Start())
	t.Cleanup(func() { require.NoError(t, te.mirror.Shutdown()) })

	te.mirrorSyncLsStore = &memstore.Store{}
	te.mirrorSyncLs = cidlink.DefaultLinkSystem()
	te.mirrorSyncLs.SetReadStorage(te.mirrorSyncLsStore)
	te.mirrorSyncLs.SetWriteStorage(te.mirrorSyncLsStore)

	te.mirrorSync = ipnisync.NewSync(te.mirrorSyncLs, nil)
	require.NoError(t, err)
	t.Cleanup(func() { te.mirrorSync.Close() })
	pubInfo := peer.AddrInfo{
		ID:    te.mirrorHost.ID(),
		Addrs: te.mirror.PublisherAddrs(),
	}
	te.mirrorSyncer, err = te.mirrorSync.NewSyncer(pubInfo)
	require.NoError(t, err)
}

func (te *testEnv) sourceAddrInfo(t *testing.T) peer.AddrInfo {
	require.NotNil(t, te.sourceHost, "start source first")
	return testutil.WaitForAddrs(te.sourceHost)
}

func (te *testEnv) startSource(t *testing.T, ctx context.Context, opts ...engine.Option) {
	var err error
	te.sourceHost, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	// Override the host, since test environment needs explicit access to it.
	opts = append(opts, engine.WithHost(te.sourceHost))
	te.source, err = engine.New(opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, te.source.Shutdown()) })
	te.sourceMhs = make(map[string][]multihash.Multihash)
	te.source.RegisterMultihashLister(te.listMultihashes)
	require.NoError(t, te.source.Start(ctx))
}

func (te *testEnv) putAdOnSource(t *testing.T, ctx context.Context, ctxID []byte, mhs []multihash.Multihash, md metadata.Metadata) cid.Cid {
	te.sourceMhs[string(ctxID)] = mhs
	adCid, err := te.source.NotifyPut(ctx, nil, ctxID, md)
	require.NoError(t, err)
	return adCid
}

func (te *testEnv) removeAdOnSource(t *testing.T, ctx context.Context, ctxID []byte) cid.Cid {
	adCid, err := te.source.NotifyRemove(ctx, "", ctxID)
	require.NoError(t, err)
	return adCid
}

func (te *testEnv) listMultihashes(_ context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
	mhs, ok := te.sourceMhs[string(contextID)]
	if !ok {
		return nil, fmt.Errorf("no multihashes found for context ID: %s", string(contextID))
	}
	return provider.SliceMultihashIterator(mhs), nil
}

func (te *testEnv) requireAdChainMirroredRecursively(t *testing.T, ctx context.Context, originalAdCid, mirroredAdCid cid.Cid) {
	// Load Ads
	original, err := te.source.GetAdv(ctx, originalAdCid)
	require.NoError(t, err)
	mirrored, err := te.syncMirrorAd(ctx, mirroredAdCid)
	require.NoError(t, err)

	te.requireAdMirrored(t, ctx, original, mirrored)

	// Assert previous ad is mirrored as expected.
	if original.PreviousID == nil {
		require.Nil(t, mirrored.PreviousID)
		return
	}
	te.requireAdChainMirroredRecursively(t, ctx, original.PreviousID.(cidlink.Link).Cid, mirrored.PreviousID.(cidlink.Link).Cid)
}

func (te *testEnv) requireAdMirrored(t *testing.T, ctx context.Context, original, mirrored *schema.Advertisement) {
	// Assert fields that should have remained the same are identical.
	require.Equal(t, original.IsRm, mirrored.IsRm)
	require.Equal(t, original.Provider, mirrored.Provider)
	require.Equal(t, original.Metadata, mirrored.Metadata)
	require.Equal(t, original.Addresses, mirrored.Addresses)
	require.Equal(t, original.ContextID, mirrored.ContextID)

	gotSigner, err := mirrored.VerifySignature()
	require.NoError(t, err)

	// In the test environment the signer of ad is either the source or the mirror depending on
	// the mirroring options or weather the PreviousID or Entries link is changed in comparison with
	// the original ad.
	// Assert one or the other accordingly.
	var wantSigner peer.ID
	if te.mirror.AlwaysReSignAds() || original.Entries != mirrored.Entries || original.PreviousID != mirrored.PreviousID {
		wantSigner = te.mirrorHost.ID()
	} else {
		wantSigner = te.sourceHost.ID()
	}
	require.Equal(t, wantSigner, gotSigner)

	// Assert entries are mirrored as expected
	te.requireEntriesMirrored(t, ctx, original.ContextID, original.Entries, mirrored.Entries)
}

func (te *testEnv) requireEntriesMirrored(t *testing.T, ctx context.Context, contextID []byte, originalEntriesLink, mirroredEntriesLink ipld.Link) {

	if originalEntriesLink == schema.NoEntries {
		require.Equal(t, schema.NoEntries, mirroredEntriesLink)
		return
	}

	// Entries link should never be nil.
	require.NotNil(t, originalEntriesLink)
	require.NotNil(t, mirroredEntriesLink)

	// Assert that the entries are sync-able from mirror which will implicitly assert that the
	// returned entries indeed correspond to the given link via block digest verification.
	err := te.mirrorSyncer.Sync(ctx, mirroredEntriesLink.(cidlink.Link).Cid, selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)

	if !te.mirror.RemapEntriesEnabled() {
		require.Equal(t, originalEntriesLink, mirroredEntriesLink)
		return
	}

	wantMhs := te.sourceMhs[string(contextID)]

	var mirroredMhIter provider.MultihashIterator
	switch te.mirror.EntriesRemapPrototype() {
	case schema.EntryChunkPrototype:
		mirroredMhIter, err = provider.EntryChunkMultihashIterator(mirroredEntriesLink, te.mirrorSyncLs)
		require.NoError(t, err)
	case hamt.HashMapRootPrototype:
		n, err := te.mirrorSyncLs.Load(ipld.LinkContext{Ctx: ctx}, mirroredEntriesLink, hamt.HashMapRootPrototype)
		require.NoError(t, err)
		root := bindnode.Unwrap(n).(*hamt.HashMapRoot)
		mirroredMhIter = provider.HamtMultihashIterator(root, te.mirrorSyncLs)
		require.NoError(t, err)
	default:
		t.Fatal("unknown entries remap prototype", te.mirror.EntriesRemapPrototype())
	}

	var gotMhs []multihash.Multihash
	for {
		next, err := mirroredMhIter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		gotMhs = append(gotMhs, next)
	}
	require.ElementsMatch(t, wantMhs, gotMhs)
}

func (te *testEnv) syncFromMirrorRecursively(ctx context.Context, c cid.Cid) error {
	exists, err := te.mirrorSyncLsStore.Has(ctx, cidlink.Link{Cid: c}.Binary())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if te.mirrorSyncer == nil {
		return errors.New("start mirror first")
	}
	return te.mirrorSyncer.Sync(ctx, c, selectorparse.CommonSelector_MatchPoint)
}

func (te *testEnv) syncMirrorAd(ctx context.Context, adCid cid.Cid) (*schema.Advertisement, error) {
	if err := te.syncFromMirrorRecursively(ctx, adCid); err != nil {
		return nil, err
	}
	n, err := te.mirrorSyncLs.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: adCid}, schema.AdvertisementPrototype)
	if err != nil {
		return nil, err
	}
	return schema.UnwrapAdvertisement(n)
}
