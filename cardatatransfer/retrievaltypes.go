package cardatatransfer

import (
	"bytes"
	"fmt"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
)

/* This file is copied from go-fil-markets types for retrieval */

//go:generate cbor-gen-for --map-encoding DealProposal DealResponse Params

// DealStatus is the status of a retrieval deal returned by a provider
// in a DealResponse
type DealStatus uint64

const (
	// DealStatusNew is a deal that nothing has happened with yet
	DealStatusNew DealStatus = iota

	// DealStatusUnsealing means the provider is unsealing data
	DealStatusUnsealing

	// DealStatusUnsealed means the provider has finished unsealing data
	DealStatusUnsealed

	// DealStatusWaitForAcceptance means we're waiting to hear back if the provider accepted our deal
	DealStatusWaitForAcceptance

	// DealStatusPaymentChannelCreating is the status set while waiting for the
	// payment channel creation to complete
	DealStatusPaymentChannelCreating

	// DealStatusPaymentChannelAddingFunds is the status when we are waiting for funds
	// to finish being sent to the payment channel
	DealStatusPaymentChannelAddingFunds

	// DealStatusAccepted means a deal has been accepted by a provider
	// and its is ready to proceed with retrieval
	DealStatusAccepted

	// DealStatusFundsNeededUnseal means a deal has been accepted by a provider
	// and payment is needed to unseal the data
	DealStatusFundsNeededUnseal

	// DealStatusFailing indicates something went wrong during a retrieval,
	// and we are cleaning up before terminating with an error
	DealStatusFailing

	// DealStatusRejected indicates the provider rejected a client's deal proposal
	// for some reason
	DealStatusRejected

	// DealStatusFundsNeeded indicates the provider needs a payment voucher to
	// continue processing the deal
	DealStatusFundsNeeded

	// DealStatusSendFunds indicates the client is now going to send funds because we reached the threshold of the last payment
	DealStatusSendFunds

	// DealStatusSendFundsLastPayment indicates the client is now going to send final funds because
	// we reached the threshold of the final payment
	DealStatusSendFundsLastPayment

	// DealStatusOngoing indicates the provider is continuing to process a deal
	DealStatusOngoing

	// DealStatusFundsNeededLastPayment indicates the provider needs a payment voucher
	// in order to complete a deal
	DealStatusFundsNeededLastPayment

	// DealStatusCompleted indicates a deal is complete
	DealStatusCompleted

	// DealStatusDealNotFound indicates an update was received for a deal that could
	// not be identified
	DealStatusDealNotFound

	// DealStatusErrored indicates a deal has terminated in an error
	DealStatusErrored

	// DealStatusBlocksComplete indicates that all blocks have been processed for the piece
	DealStatusBlocksComplete

	// DealStatusFinalizing means the last payment has been received and
	// we are just confirming the deal is complete
	DealStatusFinalizing

	// DealStatusCompleting is just an inbetween state to perform final cleanup of
	// complete deals
	DealStatusCompleting

	// DealStatusCheckComplete is used for when the provided completes without a last payment
	// requested cycle, to verify we have received all blocks
	DealStatusCheckComplete

	// DealStatusCheckFunds means we are looking at the state of funding for the channel to determine
	// if more money is incoming
	DealStatusCheckFunds

	// DealStatusInsufficientFunds indicates we have depleted funds for the retrieval payment channel
	// - we can resume after funds are added
	DealStatusInsufficientFunds

	// DealStatusPaymentChannelAllocatingLane is the status when we are making a lane for this channel
	DealStatusPaymentChannelAllocatingLane

	// DealStatusCancelling means we are cancelling an inprogress deal
	DealStatusCancelling

	// DealStatusCancelled means a deal has been cancelled
	DealStatusCancelled

	// DealStatusRetryLegacy means we're attempting the deal proposal for a second time using the legacy datatype
	DealStatusRetryLegacy

	// DealStatusWaitForAcceptanceLegacy means we're waiting to hear the results on the legacy protocol
	DealStatusWaitForAcceptanceLegacy

	// DealStatusClientWaitingForLastBlocks means that the provider has told
	// the client that all blocks were sent for the deal, and the client is
	// waiting for the last blocks to arrive. This should only happen when
	// the deal price per byte is zero (if it's not zero the provider asks
	// for final payment after sending the last blocks).
	DealStatusClientWaitingForLastBlocks

	// DealStatusPaymentChannelAddingInitialFunds means that a payment channel
	// exists from an earlier deal between client and provider, but we need
	// to add funds to the channel for this particular deal
	DealStatusPaymentChannelAddingInitialFunds

	// DealStatusErroring means that there was an error and we need to
	// do some cleanup before moving to the error state
	DealStatusErroring

	// DealStatusRejecting means that the deal was rejected and we need to do
	// some cleanup before moving to the rejected state
	DealStatusRejecting

	// DealStatusDealNotFoundCleanup means that the deal was not found and we
	// need to do some cleanup before moving to the not found state
	DealStatusDealNotFoundCleanup

	// DealStatusFinalizingBlockstore means that all blocks have been received,
	// and the blockstore is being finalized
	DealStatusFinalizingBlockstore
)

// Params are the parameters requested for a retrieval deal proposal
type Params struct {
	Selector                *cbg.Deferred // V1
	PieceCID                *cid.Cid
	PricePerByte            abi.TokenAmount
	PaymentInterval         uint64 // when to request payment
	PaymentIntervalIncrease uint64
	UnsealPrice             abi.TokenAmount
}

func (p Params) SelectorSpecified() bool {
	return p.Selector != nil && !bytes.Equal(p.Selector.Raw, cbg.CborNull)
}

// DealID is an identifier for a retrieval deal (unique to a client)
type DealID uint64

func (d DealID) String() string {
	return fmt.Sprintf("%d", d)
}

// ProviderDealID is a value that uniquely identifies a deal to the provider
type ProviderDealID struct {
	Receiver peer.ID
	DealID   DealID
}

func (p ProviderDealID) String() string {
	return fmt.Sprintf("%v/%v", p.Receiver, p.DealID)
}

// DealProposal is a proposal for a new retrieval deal
type DealProposal struct {
	PayloadCID cid.Cid
	ID         DealID
	Params
}

// Type method makes DealProposal usable as a voucher
func (dp *DealProposal) Type() datatransfer.TypeIdentifier {
	return "RetrievalDealProposal/1"
}

// DealProposalUndefined is an undefined deal proposal
var DealProposalUndefined = DealProposal{}

// DealResponse is a response to a retrieval deal proposal
type DealResponse struct {
	Status DealStatus
	ID     DealID

	// payment required to proceed
	PaymentOwed abi.TokenAmount

	Message string
}

// Type method makes DealResponse usable as a voucher result
func (dr *DealResponse) Type() datatransfer.TypeIdentifier {
	return "RetrievalDealResponse/1"
}

// DealResponseUndefined is an undefined deal response
var DealResponseUndefined = DealResponse{}
