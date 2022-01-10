package engine

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

var _ pubsub.EventTracer = (*testPubSubTracer)(nil)

// testPubSubTracer is a pubsub.EventTracer that allows the tracer to expect certain events, useful
// for testing.
type testPubSubTracer struct {
	m      sync.Mutex
	tracer func(evt *pubsubpb.TraceEvent)
}

func (tpst *testPubSubTracer) Trace(evt *pubsubpb.TraceEvent) {
	tpst.m.Lock()
	defer tpst.m.Unlock()
	if tpst.tracer != nil {
		tpst.tracer(evt)
	}
}

// requireDeliverMessageEventually checks that a message is delivered on the given topic from the given peer ID within a timeout.
func (tpst *testPubSubTracer) requireDeliverMessageEventually(from peer.ID, topic string, timeout time.Duration) <-chan bool {
	return tpst.requireOnceEventually(func(evt *pubsubpb.TraceEvent) bool {
		return pubsubpb.TraceEvent_DELIVER_MESSAGE == evt.GetType() &&
			from == peer.ID(evt.GetDeliverMessage().GetReceivedFrom()) &&
			topic == evt.GetDeliverMessage().GetTopic()
	}, timeout)
}

// requireOnceEventually applies the given check function to the trace events until one one of them
// passes the check or the timeout occurs.
// This function returns a channel that indicates whether the check passed for at least one of the
// trace events within timeout.
func (tpst *testPubSubTracer) requireOnceEventually(check func(evt *pubsubpb.TraceEvent) bool, timeout time.Duration) <-chan bool {
	// Use two separate channels, one for signalling the final result to the caller and
	// one for signalling that the check has passed.
	// The channels are separate so that we can guarantee that channels are written to exactly once
	// by the same writer and closed when written to.
	result := make(chan bool, 1)
	passed := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	go func() {
		defer close(result)
		// Await until either timeout occurs or check passes.
		select {
		case <-ctx.Done():
			result <- false
		case <-passed:
			result <- true
			cancel()
		}

		// Clear tracer when done.
		tpst.m.Lock()
		defer tpst.m.Unlock()
		tpst.tracer = nil
	}()

	tpst.m.Lock()
	defer tpst.m.Unlock()
	tpst.tracer = func(evt *pubsubpb.TraceEvent) {
		pass := check(evt)
		if pass {
			passed <- pass
			close(passed)
		}
	}
	return result
}
