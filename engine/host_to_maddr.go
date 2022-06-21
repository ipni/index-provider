package engine

import (
	"fmt"
	"net"
	"regexp"

	"github.com/multiformats/go-multiaddr"
)

func parseAddrType(host string) string {
	addr := net.ParseIP(host)
	if addr == nil {
		re, _ := regexp.Compile(`^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$`)
		if !re.MatchString(host) {
			return ""
		}
		return "dns4"
	}
	if addr.To4() != nil {
		return "ip4"
	}
	if addr.To16() != nil {
		return "ip6"
	}
	return ""
}

func hostToMultiaddr(hostport string) (multiaddr.Multiaddr, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		port = "80"
		host = hostport
	}
	if len(host) > 255 {
		return nil, fmt.Errorf("host name length cannot exceed 255")
	}
	addrType := parseAddrType(host)
	if addrType == "" {
		return nil, fmt.Errorf("unrecognized address: %q", host)
	}
	return multiaddr.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%s", addrType, host, port))
}
