package metrics

import (
	"go.opentelemetry.io/otel"
)

var meter = otel.GetMeterProvider().Meter("index-provider")
