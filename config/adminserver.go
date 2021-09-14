package config

import "time"

const (
	defaultAdminServerAddr = "/ip4/127.0.0.1/tcp/3102"
	defaultReadTimeout     = 30 * time.Second
	defaultWriteTimeout    = 30 * time.Second
)

type AdminServer struct {
	// Admin is the admin API listen address
	ListenMultiaddr string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}
