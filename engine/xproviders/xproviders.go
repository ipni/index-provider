package xproviders

import (
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// AdBuilder contains fields required for building and signing of a new ad with Extended Providers
type AdBuilder struct {
	// providerID contains a peer ID of the main provider (the one from the body of the ad)
	providerID string
	// privKey contains a private key of the main provider
	privKey crypto.PrivKey
	// addrs contains addresses of the main provider
	addrs []string
	// providers contains providers' identities, keys, metadata and addresses that will be used as extended providers in the ad.
	providers []Info
	// contextID contains optional context id
	contextID []byte
	// contextID contains optional metadata
	metadata []byte
	// override contains override flag that is false by default
	override bool
	// lastAdID contains optional last ad cid which is cid.Undef by default
	lastAdID cid.Cid
	// entries is the CID of the entries the ad applies to
	entries cid.Cid
}

// Info contains information about extended provider.
type Info struct {
	// ID contains peer ID of the extended provider
	ID string
	// Metadata contains optional metadata of the extended provider
	Metadata []byte
	// Addrs contains a list of extended provider's addresses
	Addrs []string
	// Priv contains a provtae key of the extended provider
	Priv crypto.PrivKey
}

// NewAdBuilder creates a new ExtendedProvidersAdBuilder
func NewAdBuilder(providerID peer.ID, privKey crypto.PrivKey, addrs []ma.Multiaddr) *AdBuilder {
	pub := &AdBuilder{
		providerID: providerID.String(),
		privKey:    privKey,
		addrs:      multiaddrsToStrings(addrs),
		providers:  []Info{},
		lastAdID:   cid.Undef,
	}
	return pub
}

// WithContextID sets contextID
func (pub *AdBuilder) WithContextID(contextID []byte) *AdBuilder {
	pub.contextID = contextID
	return pub
}

// WithMetadata sets metadata
func (pub *AdBuilder) WithMetadata(md []byte) *AdBuilder {
	pub.metadata = md
	return pub
}

// WithOverride sets override
func (pub *AdBuilder) WithOverride(override bool) *AdBuilder {
	pub.override = override
	return pub
}

// WithExtendedProviders sets extended providers
func (pub *AdBuilder) WithExtendedProviders(eps ...Info) *AdBuilder {
	pub.providers = append(pub.providers, eps...)
	return pub
}

// WithLastAdID sets last ad cid
func (pub *AdBuilder) WithLastAdID(lastAdID cid.Cid) *AdBuilder {
	pub.lastAdID = lastAdID
	return pub
}

// WithEntries sets the CID of the entries for the ad
func (pub *AdBuilder) WithEntries(entries cid.Cid) *AdBuilder {
	pub.entries = entries
	return pub
}

// BuildAndSign verifies and  signs a new extended provider ad. After that it can be published using engine. Identity of the main provider will be appended to the
// extended provider list automatically.
func (pub *AdBuilder) BuildAndSign() (*schema.Advertisement, error) {
	if len(pub.contextID) == 0 && pub.override {
		return nil, errors.New("override is true for empty context")
	}

	entries := schema.NoEntries
	if pub.entries != cid.Undef {
		entries = cidlink.Link{Cid: pub.entries}
	}

	adv := schema.Advertisement{
		Provider:  pub.providerID,
		Entries:   entries,
		Addresses: pub.addrs,
		ContextID: pub.contextID,
		Metadata:  pub.metadata,
		ExtendedProvider: &schema.ExtendedProvider{
			Override: pub.override,
		},
	}

	epMap := map[string]Info{}
	if len(pub.providers) != 0 {
		for _, epInfo := range pub.providers {
			if len(epInfo.Addrs) == 0 {
				return nil, errors.New("addresses of an extended provider can not be empty")
			}
			_, err := peer.Decode(epInfo.ID)
			if err != nil {
				return nil, errors.New("invalid extended provider peer id")
			}
			adv.ExtendedProvider.Providers = append(adv.ExtendedProvider.Providers, schema.Provider{
				ID:        epInfo.ID,
				Addresses: epInfo.Addrs,
				Metadata:  epInfo.Metadata,
			})
			epMap[epInfo.ID] = epInfo
		}

		// The main provider has to be on the extended list too
		if _, ok := epMap[pub.providerID]; !ok {
			adv.ExtendedProvider.Providers = append(adv.ExtendedProvider.Providers, schema.Provider{
				ID:        pub.providerID,
				Addresses: pub.addrs,
				Metadata:  pub.metadata,
			})
		}
	}

	if pub.lastAdID != cid.Undef {
		prev := ipld.Link(cidlink.Link{Cid: pub.lastAdID})
		adv.PreviousID = prev
	}

	err := adv.SignWithExtendedProviders(pub.privKey, func(provId string) (crypto.PrivKey, error) {
		epInfo, ok := epMap[provId]
		if !ok {
			return nil, errors.New("unknown provider")
		}
		return epInfo.Priv, nil
	})

	if err != nil {
		return nil, err
	}

	return &adv, nil
}

// NewInfo allows to create a new ExtendedProviderInfo providing type safety around its fields
func NewInfo(peerID peer.ID, priv crypto.PrivKey, metadata []byte, addrs []ma.Multiaddr) Info {
	return Info{
		ID:       peerID.String(),
		Metadata: metadata,
		Addrs:    multiaddrsToStrings(addrs),
		Priv:     priv,
	}
}

func multiaddrsToStrings(addrs []ma.Multiaddr) []string {
	var stringAddrs []string
	for _, addr := range addrs {
		stringAddrs = append(stringAddrs, addr.String())
	}
	return stringAddrs
}
