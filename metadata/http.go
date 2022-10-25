package metadata

import "github.com/multiformats/go-multicodec"

func HTTPV1() Protocol {
	return &Unknown{
		Code: multicodec.Http,
	}
}
