package provider

import "errors"

var (
	// ErrNoMultihashLister signals that no provider.MultihashLister is registered for lookup.
	ErrNoMultihashLister = errors.New("no multihash lister is registered")

	// ErrContextIDNotFound signals that no item is associated to the given context ID.
	ErrContextIDNotFound = errors.New("context ID not found")

	// ErrAlreadyAdvertised signals that an advertisement for identical content was already
	// published.
	ErrAlreadyAdvertised = errors.New("advertisement already published")
)
