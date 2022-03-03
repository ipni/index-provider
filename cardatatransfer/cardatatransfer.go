package cardatatransfer

import (
	"bytes"
	"errors"
	"fmt"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/index-provider/cardatatransfer/stores"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/supplier"
)

var log = logging.Logger("car-data-transfer")

var allSelectorBytes []byte

func init() {
	buf := new(bytes.Buffer)
	_ = dagcbor.Encode(selectorparse.CommonSelector_ExploreAllRecursively, buf)
	allSelectorBytes = buf.Bytes()
}

type BlockStoreSupplier interface {
	ReadOnlyBlockstore(contextID []byte) (supplier.ClosableBlockstore, error)
}

type carDataTransfer struct {
	dt       datatransfer.Manager
	supplier BlockStoreSupplier
	stores   *stores.ReadOnlyBlockstores
}

// TBD: this is really for internal use only
// -- maybe we find something that doesn't overlap a protocol range?

const ContextIDCodec multicodec.Code = 0x300001

func StartCarDataTransfer(dt datatransfer.Manager, supplier BlockStoreSupplier) error {
	cdt := &carDataTransfer{
		dt:       dt,
		supplier: supplier,
		stores:   stores.NewReadOnlyBlockstores(),
	}
	err := dt.RegisterVoucherType(&DealProposal{}, cdt)
	if err != nil {
		return err
	}
	err = dt.RegisterVoucherResultType(&DealResponse{})
	if err != nil {
		return err
	}
	err = dt.RegisterTransportConfigurer(&DealProposal{}, cdt.transportConfigurer)
	if err != nil {
		return err
	}
	dt.SubscribeToEvents(cdt.eventListener)
	return nil
}

func MetadataFromContextID(contextID []byte) (stiapi.Metadata, error) {
	pieceCid, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(ContextIDCodec),
		MhType:   multihash.IDENTITY,
		MhLength: -1,
	}.Sum(contextID)
	if err != nil {
		return stiapi.Metadata{}, err
	}
	filecoinV1Metadata := &metadata.GraphsyncFilecoinV1Metadata{
		PieceCID:      pieceCid,
		VerifiedDeal:  true,
		FastRetrieval: true,
	}
	return filecoinV1Metadata.ToIndexerMetadata()
}

// ValidatePush validates a push request received from the peer that will send data
func (cdt *carDataTransfer) ValidatePush(isRestart bool, _ datatransfer.ChannelID, sender peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	return nil, errors.New("no pushes accepted")
}

// ValidatePull validates a pull request received from the peer that will receive data
func (cdt *carDataTransfer) ValidatePull(isRestart bool, _ datatransfer.ChannelID, receiver peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	proposal, ok := voucher.(*DealProposal)
	if !ok {
		return nil, errors.New("wrong voucher type")
	}

	// Check the proposal CID matches
	if proposal.PayloadCID != baseCid {
		return nil, errors.New("incorrect CID for this proposal")
	}

	// Check the proposal selector matches
	buf := new(bytes.Buffer)
	err := dagcbor.Encode(selector, buf)
	if err != nil {
		return nil, err
	}
	bytesCompare := allSelectorBytes
	if proposal.SelectorSpecified() {
		bytesCompare = proposal.Selector.Raw
	}
	if !bytes.Equal(buf.Bytes(), bytesCompare) {
		return nil, errors.New("incorrect selector for this proposal")
	}

	// If the validation is for a restart request, return nil, which means
	// the data-transfer should not be explicitly paused or resumed
	if isRestart {
		return nil, nil
	}

	// attempt to setup the deal
	providerDealID := ProviderDealID{DealID: proposal.ID, Receiver: receiver}

	status, err := cdt.attemptAcceptDeal(providerDealID, proposal)

	response := DealResponse{
		ID:     proposal.ID,
		Status: status,
	}

	if err != nil {
		response.Message = err.Error()
		return &response, err
	}
	return &response, nil
}

func (cdt *carDataTransfer) attemptAcceptDeal(providerDealID ProviderDealID, proposal *DealProposal) (DealStatus, error) {
	if proposal.PieceCID == nil {
		return DealStatusErrored, errors.New("must specific piece CID")
	}

	// get contextID
	prefix := proposal.PieceCID.Prefix()
	if prefix.Codec != uint64(ContextIDCodec) {
		return DealStatusErrored, errors.New("incorrect Piece CID codec")
	}
	if prefix.MhType != multihash.IDENTITY {
		return DealStatusErrored, errors.New("piece CID must be an identity CI")
	}
	dmh, err := multihash.Decode(proposal.PieceCID.Hash())
	if err != nil {
		return DealStatusErrored, errors.New("unable to decode piece CID")
	}
	contextID := dmh.Digest

	// read blockstore from supplier
	bs, err := cdt.supplier.ReadOnlyBlockstore(contextID)
	if err != nil {
		return DealStatusErrored, fmt.Errorf("error reading blockstore: %w", err)
	}
	cdt.stores.Track(providerDealID.String(), bs)
	return DealStatusAccepted, nil
}

func checkTermination(event datatransfer.Event, channelState datatransfer.ChannelState) bool {
	return channelState.Status() == datatransfer.Completed ||
		event.Code == datatransfer.Disconnected ||
		event.Code == datatransfer.Error ||
		event.Code == datatransfer.Cancel
}

func (cdt *carDataTransfer) eventListener(event datatransfer.Event, channelState datatransfer.ChannelState) {
	dealProposal, ok := channelState.Voucher().(*DealProposal)
	// if this event is for a transfer not related to storage, ignore
	if !ok {
		return
	}

	providerDealID := ProviderDealID{DealID: dealProposal.ID, Receiver: channelState.Recipient()}

	if checkTermination(event, channelState) {
		err := cdt.stores.Untrack(providerDealID.String())
		if err != nil {
			log.Errorf("termination error: %s", err)
		}
	}
}

// StoreConfigurableTransport defines the methods needed to
// configure a data transfer transport use a unique store for a given request
type StoreConfigurableTransport interface {
	UseStore(datatransfer.ChannelID, ipld.LinkSystem) error
}

func (ctd *carDataTransfer) transportConfigurer(channelID datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
	dealProposal, ok := voucher.(*DealProposal)
	if !ok {
		return
	}
	gsTransport, ok := transport.(StoreConfigurableTransport)
	if !ok {
		return
	}
	providerDealID := ProviderDealID{Receiver: channelID.Initiator, DealID: dealProposal.ID}
	store, err := ctd.stores.Get(providerDealID.String())
	if err != nil {
		log.Errorf("attempting to configure data store: %s", err)
		return
	}
	if store == nil {
		return
	}
	err = gsTransport.UseStore(channelID, storeutil.LinkSystemForBlockstore(store))
	if err != nil {
		log.Errorf("attempting to configure data store: %s", err)
	}
}
