package utils

import "time"

type Options struct {
	ListenAddr   string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}
