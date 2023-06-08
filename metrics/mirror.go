package metrics

import (
	"go.opentelemetry.io/otel/metric"
)

var Mirror struct {
	SyncDuration    metric.Int64Histogram
	ProcessDuration metric.Int64Histogram
}

func init() {
	var err error
	if Mirror.ProcessDuration, err = meter.Int64Histogram(
		"index-provider/mirror/process_duration",
		metric.WithUnit("ms"),
		metric.WithDescription("The time taken to process ad mirroring in milliseconds"),
	); err != nil {
		panic(err)
	}
	if Mirror.SyncDuration, err = meter.Int64Histogram(
		"index-provider/mirror/sync_duration",
		metric.WithUnit("ms"),
		metric.WithDescription("The time taken to sync content in milliseconds"),
	); err != nil {
		panic(err)
	}
}
