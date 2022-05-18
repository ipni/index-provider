package policy

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
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
	if err != nil {
		t.Fatal(err)
	}

	_, err = New(true, except)
	if err != nil {
		t.Fatal(err)
	}

	except = append(except, "bad ID")
	_, err = New(false, except)
	if err == nil {
		t.Error("expected error with bad except ID")
	}

	_, err = New(false, nil)
	if err != nil {
		t.Error(err)
	}

	_, err = New(true, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestPolicyAccess(t *testing.T) {
	allow := false
	except := []string{exceptIDStr}

	p, err := New(allow, except)
	if err != nil {
		t.Fatal(err)
	}

	if p.Allowed(otherID) {
		t.Error("peer ID should not be allowed by policy")
	}
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed")
	}

	p.Allow(otherID)
	if !p.Allowed(otherID) {
		t.Error("peer ID should be allowed by policy")
	}

	p.Block(exceptID)
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}

	allow = true
	newPol, err := New(allow, except)
	if err != nil {
		t.Fatal(err)
	}
	p.Copy(newPol)

	if !p.Allowed(otherID) {
		t.Error("peer ID should be allowed by policy")
	}
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}

	p.Allow(exceptID)
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed by policy")
	}

	p.Block(otherID)
	if p.Allowed(otherID) {
		t.Error("peer ID should not be allowed")
	}
}
