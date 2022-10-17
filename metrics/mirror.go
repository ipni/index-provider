package metrics

import (
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
)

var Mirror struct {
	SyncDuration    syncint64.Histogram
	ProcessDuration syncint64.Histogram
}

func init() {
	var err error
	if Mirror.ProcessDuration, err = meter.SyncInt64().Histogram(
		"index-provider/mirror/process_duration",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("The time taken to process ad mirroring in milliseconds"),
	); err != nil {
		panic(err)
	}
	if Mirror.SyncDuration, err = meter.SyncInt64().Histogram(
		"index-provider/mirror/sync_duration",
		instrument.WithUnit(unit.Milliseconds),
		instrument.WithDescription("The time taken to sync content in milliseconds"),
	); err != nil {
		panic(err)
	}
}
