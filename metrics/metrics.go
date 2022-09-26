package metrics

import (
	"go.opentelemetry.io/otel/metric/global"
)

var meter = global.MeterProvider().Meter("index-provider")
