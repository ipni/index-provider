package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHostToMultiaddr(t *testing.T) {
	m, err := hostToMultiaddr("google.com")
	require.NoError(t, err)
	require.Equal(t, "/dns4/google.com/tcp/80", m.String())

	m, err = hostToMultiaddr("192.168.1.0")
	require.NoError(t, err)
	require.Equal(t, "/ip4/192.168.1.0/tcp/80", m.String())

	m, err = hostToMultiaddr("192.168.1.0:3104")
	require.NoError(t, err)
	require.Equal(t, "/ip4/192.168.1.0/tcp/3104", m.String())

	m, err = hostToMultiaddr("0:0:0:0:0:0:0:1")
	require.NoError(t, err)
	require.Equal(t, "/ip6/::1/tcp/80", m.String())

	m, err = hostToMultiaddr("ns.google.com:8888")
	require.NoError(t, err)
	require.Equal(t, "/dns4/ns.google.com/tcp/8888", m.String())

	// Port too high.
	_, err = hostToMultiaddr("ns.google.com:65537")
	require.Error(t, err)

	// Port too high.
	_, err = hostToMultiaddr("10.1.1.44:99999")
	require.Error(t, err)

	// Bad character in DNS name.
	_, err = hostToMultiaddr("foo/bar")
	require.Error(t, err)

	// DNS name ends with ".".
	_, err = hostToMultiaddr("google.com.")
	require.Error(t, err)

	// DNS name begins with "-".
	_, err = hostToMultiaddr("-google.com")
	require.Error(t, err)
}
