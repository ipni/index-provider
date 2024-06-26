package xproviders_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/index-provider/engine"
	ep "github.com/ipni/index-provider/engine/xproviders"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const testTimeout = 30 * time.Second

func TestPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	contextID := []byte("test-context")
	addrs := random.Multiaddrs(2)
	addrsStr := []string{addrs[0].String(), addrs[1].String()}
	metadata := []byte("thisismeta")
	eps := make([]ep.Info, 2)
	epIds := make([]peer.ID, len(eps))

	eng, err := engine.New()
	require.NoError(t, err)
	err = eng.Start(ctx)
	require.NoError(t, err)
	defer eng.Shutdown()

	providerID, priv, _ := random.Identity()

	for i := 0; i < len(eps); i++ {
		epID, ep := randomExtendedProvider()
		eps[i] = ep
		epIds[i] = epID
	}

	override := true
	adv, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(eps...).
		WithOverride(true).
		WithContextID(contextID).
		WithMetadata(metadata).
		BuildAndSign()
	require.NoError(t, err)
	advPeerID, err := adv.VerifySignature()
	require.NoError(t, err)

	// verify that we can publish successfully
	c, err := eng.Publish(ctx, *adv)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, c)

	require.Equal(t, providerID, advPeerID)
	require.Equal(t, addrsStr, adv.Addresses)
	require.Equal(t, contextID, adv.ContextID)
	require.Equal(t, schema.NoEntries, adv.Entries)
	require.Equal(t, false, adv.IsRm)
	require.Equal(t, metadata, adv.Metadata)
	require.Equal(t, providerID.String(), adv.Provider)
	require.Equal(t, override, adv.ExtendedProvider.Override)
	require.Equal(t, 3, len(adv.ExtendedProvider.Providers))

	ep1 := eps[0]
	ep2 := eps[1]
	for _, p := range adv.ExtendedProvider.Providers {
		switch p.ID {
		case ep1.ID:
			require.Equal(t, ep1.Addrs, p.Addresses)
			require.Equal(t, ep1.Metadata, p.Metadata)
		case ep2.ID:
			require.Equal(t, ep2.Addrs, p.Addresses)
			require.Equal(t, ep2.Metadata, p.Metadata)
		case providerID.String():
			require.Equal(t, addrsStr, p.Addresses)
			require.Equal(t, metadata, p.Metadata)
		default:
			panic("unknown provider")
		}
	}
}

func TestMainProviderShouldNotBeAddedAsExtendedIfItsAlreadyOnTheList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	contextID := []byte("test-context")

	addrs := random.Multiaddrs(2)
	addrsStr := []string{addrs[0].String(), addrs[1].String()}
	metadata := []byte("thisismeta")

	eng, err := engine.New()
	require.NoError(t, err)
	err = eng.Start(ctx)
	require.NoError(t, err)
	defer eng.Shutdown()

	providerID, priv, _ := random.Identity()

	override := true
	adv, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(ep.NewInfo(providerID, priv, metadata, addrs)).
		WithOverride(true).
		WithContextID(contextID).
		WithMetadata(metadata).
		BuildAndSign()
	require.NoError(t, err)
	advPeerID, err := adv.VerifySignature()
	require.NoError(t, err)

	// verify that we can publish successfully
	c, err := eng.Publish(ctx, *adv)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, c)

	require.Equal(t, providerID, advPeerID)
	require.Equal(t, addrsStr, adv.Addresses)
	require.Equal(t, contextID, adv.ContextID)
	require.Equal(t, schema.NoEntries, adv.Entries)
	require.Equal(t, false, adv.IsRm)
	require.Equal(t, metadata, adv.Metadata)
	require.Equal(t, providerID.String(), adv.Provider)
	require.Equal(t, override, adv.ExtendedProvider.Override)
	require.Equal(t, 1, len(adv.ExtendedProvider.Providers))

	ep := adv.ExtendedProvider.Providers[0]
	require.Equal(t, addrsStr, ep.Addresses)
	require.Equal(t, metadata, ep.Metadata)
	require.Equal(t, providerID.String(), ep.ID)
}

func TestExtendedProvidersShouldNotAllowEmptyAddresses(t *testing.T) {
	addrs := random.Multiaddrs(2)
	metadata := []byte("thisismeta")

	providerID, priv, _ := random.Identity()

	_, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(ep.NewInfo(providerID, priv, metadata, []multiaddr.Multiaddr{})).
		WithOverride(true).
		WithContextID([]byte("test-context")).
		WithMetadata(metadata).
		BuildAndSign()
	require.Error(t, err, "addresses of an extended provider can not be empty")
}

func TestExtendedProvidersShouldAllowEmptyMetadata(t *testing.T) {
	addrs := random.Multiaddrs(2)
	metadata := []byte("thisismeta")

	providerID, priv, _ := random.Identity()

	_, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(ep.NewInfo(providerID, priv, []byte{}, addrs)).
		WithOverride(true).
		WithContextID([]byte("test-context")).
		WithMetadata(metadata).
		BuildAndSign()
	require.NoError(t, err)
}

func TestExtendedProvidersShouldNotAllowInvalidPeerIDs(t *testing.T) {
	addrs := random.Multiaddrs(2)
	metadata := []byte("thisismeta")

	providerID, priv, _ := random.Identity()

	_, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(ep.NewInfo("invalid", priv, []byte{}, addrs)).
		WithOverride(true).
		WithContextID([]byte("test-context")).
		WithMetadata(metadata).
		BuildAndSign()
	require.Error(t, err, "invalid extended provider peer id")
}

func TestExtendedProvidersShouldAllowEntries(t *testing.T) {
	addrs := random.Multiaddrs(1)
	providerID, priv, _ := random.Identity()
	entries := random.Cids(1)[0]

	ad, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithEntries(entries).
		BuildAndSign()
	require.NoError(t, err)

	_, err = ad.VerifySignature()
	require.NoError(t, err)

	require.Equal(t, cidlink.Link{Cid: entries}, ad.Entries)
}

func TestZeroExtendedProvidersShouldStillCreateExtendedProvidersField(t *testing.T) {
	addrs := random.Multiaddrs(2)
	metadata := []byte("thisismeta")

	providerID, priv, _ := random.Identity()

	ad, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithOverride(true).
		WithContextID([]byte("test-context")).
		WithMetadata(metadata).
		BuildAndSign()
	require.NoError(t, err)
	require.NotNil(t, ad.ExtendedProvider)
	require.Empty(t, ad.ExtendedProvider.Providers)
}

func TestMainProviderShouldNotBeAddedAsExtendedIfThereAreNoOthers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	contextID := []byte("test-context")
	addrs := random.Multiaddrs(2)
	addrsStr := []string{addrs[0].String(), addrs[1].String()}
	metadata := []byte("thisismeta")

	eng, err := engine.New()
	require.NoError(t, err)
	err = eng.Start(ctx)
	require.NoError(t, err)
	defer eng.Shutdown()

	providerID, priv, _ := random.Identity()

	override := true
	adv, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithOverride(true).
		WithContextID(contextID).
		WithMetadata(metadata).
		BuildAndSign()
	require.NoError(t, err)
	advPeerID, err := adv.VerifySignature()
	require.NoError(t, err)

	// verify that we can publish successfully
	c, err := eng.Publish(ctx, *adv)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, c)

	require.Equal(t, providerID, advPeerID)
	require.Equal(t, addrsStr, adv.Addresses)
	require.Equal(t, contextID, adv.ContextID)
	require.Equal(t, schema.NoEntries, adv.Entries)
	require.Equal(t, false, adv.IsRm)
	require.Equal(t, metadata, adv.Metadata)
	require.Equal(t, providerID.String(), adv.Provider)
	require.Equal(t, override, adv.ExtendedProvider.Override)
	require.Equal(t, 0, len(adv.ExtendedProvider.Providers))
}

func TestPublishFailsIfOverrideIsTrueWithNoContextId(t *testing.T) {
	addrs := random.Multiaddrs(2)
	metadata := []byte("thisismeta")
	eps := make([]ep.Info, 2)
	epIds := make([]peer.ID, len(eps))
	for i := 0; i < len(eps); i++ {
		epID, ep := randomExtendedProvider()
		eps[i] = ep
		epIds[i] = epID
	}
	providerID, priv, _ := random.Identity()

	_, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(eps...).
		WithOverride(true).
		WithMetadata(metadata).
		BuildAndSign()

	require.Error(t, err, "override is true for empty context")
}

func randomExtendedProvider() (peer.ID, ep.Info) {
	providerID, priv, _ := random.Identity()
	metadata := []byte("thisismeta")
	addrs := random.Multiaddrs(2)
	return providerID, ep.NewInfo(providerID, priv, metadata, addrs)
}
