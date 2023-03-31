package cardatatransfer

import (
	"errors"
	"fmt"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtgs "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/ipni/index-provider/cardatatransfer/stores"
	"github.com/ipni/index-provider/supplier"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("car-data-transfer")

type ProviderDealID struct {
	DealID   retrievaltypes.DealID
	Receiver peer.ID
}

func (p ProviderDealID) String() string {
	return fmt.Sprintf("%v/%v", p.Receiver, p.DealID)
}

type BlockStoreSupplier interface {
	ReadOnlyBlockstore(contextID []byte) (supplier.ClosableBlockstore, error)
}

type carDataTransfer struct {
	dt       datatransfer.Manager
	supplier BlockStoreSupplier
	stores   *stores.ReadOnlyBlockstores
}

func StartCarDataTransfer(dt datatransfer.Manager, supplier BlockStoreSupplier) error {
	cdt := &carDataTransfer{
		dt:       dt,
		supplier: supplier,
		stores:   stores.NewReadOnlyBlockstores(),
	}
	err := dt.RegisterVoucherType(retrievaltypes.DealProposalType, cdt)
	if err != nil {
		return err
	}
	err = dt.RegisterTransportConfigurer(retrievaltypes.DealProposalType, cdt.transportConfigurer)
	if err != nil {
		return err
	}
	dt.SubscribeToEvents(cdt.eventListener)
	return nil
}

func TransportFromContextID(contextID []byte) (metadata.Protocol, error) {
	pieceCid, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.TransportGraphsyncFilecoinv1),
		MhType:   multihash.IDENTITY,
		MhLength: -1,
	}.Sum(contextID)
	if err != nil {
		return nil, err
	}
	return &metadata.GraphsyncFilecoinV1{
		PieceCID:      pieceCid,
		VerifiedDeal:  true,
		FastRetrieval: true,
	}, nil
}

// ValidatePush validates a push request received from the peer that will send data
func (cdt *carDataTransfer) ValidatePush(_ datatransfer.ChannelID, sender peer.ID, voucher datamodel.Node, baseCid cid.Cid, selector ipld.Node) (datatransfer.ValidationResult, error) {
	return datatransfer.ValidationResult{}, errors.New("no pushes accepted")
}

func rejectProposal(proposal *retrievaltypes.DealProposal, status retrievaltypes.DealStatus, reason string) (datatransfer.ValidationResult, error) {
	vr := (&retrievaltypes.DealResponse{
		ID:      proposal.ID,
		Status:  status,
		Message: reason,
	}).AsVoucher()
	return datatransfer.ValidationResult{
		Accepted:      false,
		VoucherResult: &vr,
	}, nil
}

// ValidatePull validates a pull request received from the peer that will receive data
func (cdt *carDataTransfer) ValidatePull(_ datatransfer.ChannelID, receiver peer.ID, voucher datamodel.Node, baseCid cid.Cid, selector ipld.Node) (datatransfer.ValidationResult, error) {

	proposal, err := retrievaltypes.DealProposalFromNode(voucher)
	if err != nil {
		return datatransfer.ValidationResult{}, err
	}

	// Check the proposal CID matches
	if proposal.PayloadCID != baseCid {
		return rejectProposal(proposal, retrievaltypes.DealStatusRejected, "incorrect CID for this proposal")
	}

	// Check the proposal selector matches
	sel := selectorparse.CommonSelector_ExploreAllRecursively
	if proposal.SelectorSpecified() {
		sel = proposal.Selector.Node
	}
	if !ipld.DeepEqual(sel, selector) {
		return rejectProposal(proposal, retrievaltypes.DealStatusRejected, "incorrect selector specified for this proposal")
	}

	// attempt to setup the deal
	providerDealID := ProviderDealID{DealID: proposal.ID, Receiver: receiver}

	status, err := cdt.attemptAcceptDeal(providerDealID, proposal)

	response := retrievaltypes.DealResponse{
		ID:     proposal.ID,
		Status: status,
	}

	accepted := true
	if err != nil {
		response.Message = err.Error()
		accepted = false
	}
	vr := response.AsVoucher()
	return datatransfer.ValidationResult{
		Accepted:      accepted,
		VoucherResult: &vr,
	}, nil
}

func (cdt *carDataTransfer) ValidateRestart(channelID datatransfer.ChannelID, channelState datatransfer.ChannelState) (datatransfer.ValidationResult, error) {
	voucher := channelState.Voucher()
	proposal, err := retrievaltypes.DealProposalFromNode(voucher.Voucher)
	if err != nil {
		return datatransfer.ValidationResult{}, errors.New("wrong voucher type")
	}
	providerDealID := ProviderDealID{DealID: proposal.ID, Receiver: channelState.OtherPeer()}

	status, err := cdt.attemptAcceptDeal(providerDealID, proposal)

	response := retrievaltypes.DealResponse{
		ID:     proposal.ID,
		Status: status,
	}

	accepted := true
	if err != nil {
		response.Message = err.Error()
		accepted = false
	}
	vr := response.AsVoucher()
	return datatransfer.ValidationResult{
		Accepted:      accepted,
		VoucherResult: &vr,
	}, nil

}
func (cdt *carDataTransfer) attemptAcceptDeal(providerDealID ProviderDealID, proposal *retrievaltypes.DealProposal) (retrievaltypes.DealStatus, error) {
	if proposal.PieceCID == nil {
		return retrievaltypes.DealStatusErrored, errors.New("must specific piece CID")
	}

	// get contextID
	prefix := proposal.PieceCID.Prefix()
	if prefix.Codec != uint64(multicodec.TransportGraphsyncFilecoinv1) {
		return retrievaltypes.DealStatusErrored, errors.New("incorrect Piece CID codec")
	}
	if prefix.MhType != multihash.IDENTITY {
		return retrievaltypes.DealStatusErrored, errors.New("piece CID must be an identity CI")
	}
	dmh, err := multihash.Decode(proposal.PieceCID.Hash())
	if err != nil {
		return retrievaltypes.DealStatusErrored, errors.New("unable to decode piece CID")
	}
	contextID := dmh.Digest

	// read blockstore from supplier
	bs, err := cdt.supplier.ReadOnlyBlockstore(contextID)
	if err != nil {
		return retrievaltypes.DealStatusErrored, fmt.Errorf("error reading blockstore: %w", err)
	}
	cdt.stores.Track(providerDealID.String(), bs)
	return retrievaltypes.DealStatusAccepted, nil
}

func checkTermination(event datatransfer.Event, channelState datatransfer.ChannelState) bool {
	return channelState.Status() == datatransfer.Completed ||
		event.Code == datatransfer.Disconnected ||
		event.Code == datatransfer.Error ||
		event.Code == datatransfer.Cancel
}

func (cdt *carDataTransfer) eventListener(event datatransfer.Event, channelState datatransfer.ChannelState) {
	dealProposal, err := retrievaltypes.DealProposalFromNode(channelState.Voucher().Voucher)
	// if this event is for a transfer not related to storage, ignore
	if err != nil {
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

func (ctd *carDataTransfer) transportConfigurer(channelID datatransfer.ChannelID, voucher datatransfer.TypedVoucher) []datatransfer.TransportOption {

	dealProposal, err := retrievaltypes.DealProposalFromNode(voucher.Voucher)
	if err != nil {
		return nil
	}

	providerDealID := ProviderDealID{Receiver: channelID.Initiator, DealID: dealProposal.ID}
	store, err := ctd.stores.Get(providerDealID.String())
	if err != nil {
		log.Errorf("attempting to configure data store: %s", err)
		return nil
	}
	if store == nil {
		return nil
	}
	return []datatransfer.TransportOption{dtgs.UseStore(storeutil.LinkSystemForBlockstore(store))}
}
