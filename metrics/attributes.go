package metrics

import "go.opentelemetry.io/otel/attribute"

var Attributes struct {
	StatusFailure attribute.KeyValue
	StatusSuccess attribute.KeyValue
}

func init() {
	Attributes.StatusFailure = attribute.String("status", "failure")
	Attributes.StatusSuccess = attribute.String("status", "success")
}
