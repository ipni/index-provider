package policy

import (
	"fmt"
	"sync"

	"github.com/ipni/index-provider/engine/peerutil"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Policy struct {
	allow   peerutil.Policy
	rwmutex sync.RWMutex
}

func New(allow bool, except []string) (*Policy, error) {
	pol, err := peerutil.NewPolicyStrings(allow, except)
	if err != nil {
		return nil, fmt.Errorf("bad allow policy: %s", err)
	}

	return &Policy{
		allow: pol,
	}, nil
}

// Allowed returns true if the policy allows the peer to sync content.
func (p *Policy) Allowed(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.allow.Eval(peerID)
}

// Allow alters the policy to allow the specified peer.  Returns true if the
// policy needed to be updated.
func (p *Policy) Allow(peerID peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()
	return p.allow.SetPeer(peerID, true)
}

// Block alters the policy to not allow the specified peer.  Returns true if
// the policy needed to be updated.
func (p *Policy) Block(peerID peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()
	return p.allow.SetPeer(peerID, false)
}

// Copy copies another policy.
func (p *Policy) Copy(other *Policy) {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	other.rwmutex.RLock()
	p.allow = other.allow
	other.rwmutex.RUnlock()
}

// ToConfig converts a Policy into a config.Policy.
func (p *Policy) ToConfig() (bool, []string) {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	return p.allow.Default(), p.allow.ExceptStrings()
}
