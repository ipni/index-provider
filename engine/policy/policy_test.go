package policy

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const (
	exceptIDStr = "12D3KooWK7CTS7cyWi51PeNE3cTjS2F2kDCZaQVU4A5xBmb9J1do"
	otherIDStr  = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
)

var (
	exceptID peer.ID
	otherID  peer.ID
)

func init() {
	var err error
	exceptID, err = peer.Decode(exceptIDStr)
	if err != nil {
		panic(err)
	}
	otherID, err = peer.Decode(otherIDStr)
	if err != nil {
		panic(err)
	}
}

func TestNewPolicy(t *testing.T) {
	except := []string{exceptIDStr}

	_, err := New(false, except)
	require.NoError(t, err)

	_, err = New(true, except)
	require.NoError(t, err)

	except = append(except, "bad ID")
	_, err = New(false, except)
	require.Error(t, err, "expected error with bad except ID")

	_, err = New(false, nil)
	require.NoError(t, err)

	_, err = New(true, nil)
	require.NoError(t, err)
}

func TestPolicyAccess(t *testing.T) {
	allow := false
	except := []string{exceptIDStr}

	p, err := New(allow, except)
	require.NoError(t, err)

	require.False(t, p.Allowed(otherID), "peer ID should not be allowed by policy")
	require.True(t, p.Allowed(exceptID), "peer ID should be allowed")

	p.Allow(otherID)
	require.True(t, p.Allowed(otherID), "peer ID should be allowed by policy")

	p.Block(exceptID)
	require.False(t, p.Allowed(exceptID), "peer ID should not be allowed")

	allow = true
	newPol, err := New(allow, except)
	require.NoError(t, err)
	p.Copy(newPol)

	require.True(t, p.Allowed(otherID), "peer ID should be allowed by policy")
	require.False(t, p.Allowed(exceptID), "peer ID should not be allowed")

	p.Allow(exceptID)
	require.True(t, p.Allowed(exceptID), "peer ID should be allowed by policy")

	p.Block(otherID)
	require.False(t, p.Allowed(otherID), "peer ID should not be allowed")
}
