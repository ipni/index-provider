package libp2pserver

import (
	"context"
	"io"
	"time"

	"github.com/filecoin-project/index-provider/p2putil"
	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
)

var log = logging.Logger("provider/libp2pserver")

// Idle time before the stream is closed
const streamIdleTimeout = 1 * time.Minute

// Handler for libp2p server.
type Handler interface {
	HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte) (proto.Message, error)
	ProtocolID() protocol.ID
}

// Server handles client requests over libp2p
type Server struct {
	ctx     context.Context
	handler Handler
	selfID  peer.ID
}

// ID returns the peer.ID of the protocol server.
func (s *Server) ID() peer.ID {
	return s.selfID
}

// New creates a new libp2p Server
func New(ctx context.Context, h host.Host, messageHandler Handler) *Server {
	s := &Server{
		ctx:     ctx,
		handler: messageHandler,
		selfID:  h.ID(),
	}

	// Set handler for each announced protocol
	h.SetStreamHandler(messageHandler.ProtocolID(), s.handleNewStream)

	return s
}

// handleNewStream implements the network.StreamHandler
func (s *Server) handleNewStream(stream network.Stream) {
	if s.handleNewMessages(stream) {
		// If we exited without error, close gracefully.
		_ = stream.Close()
	} else {
		// otherwise, send an error.
		_ = stream.Reset()
	}
}

// Returns true on orderly completion of writes (so we can Shutdown the stream conveniently).
func (s *Server) handleNewMessages(stream network.Stream) bool {
	ctx := s.ctx
	handler := s.handler
	r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)

	mPeer := stream.Conn().RemotePeer()

	timer := time.AfterFunc(streamIdleTimeout, func() { _ = stream.Reset() })
	defer timer.Stop()

	for {
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			return err == io.EOF
		}
		timer.Reset(streamIdleTimeout)

		resp, err := handler.HandleMessage(ctx, mPeer, msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			log.Errorf("Error handling request: %v", err)
			return true
		}

		// send out response msg
		err = p2putil.WriteMsg(stream, resp)
		if err != nil {
			log.Errorf("Error writing message: %v", err)
			return false
		}
	}
}
