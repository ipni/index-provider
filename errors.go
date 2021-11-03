package provider

import "errors"

var (
	// ErrNoCallback is thrown when no callback has been defined.
	ErrNoCallback = errors.New("no callback is registered")

	// ErrContextIDNotFound signals that no item is associated to the given context ID.
	ErrContextIDNotFound = errors.New("context ID not found")

	// ErrAlreadyAdvertised indicates that an advertisement for identical
	// content was already published.
	ErrAlreadyAdvertised = errors.New("advertisement already published")
)
