package delegatedrouting

import (
	"sync/atomic"
	"time"
)

type statsReporter struct {
	s                    *stats
	totalCidsFunc        func() int
	totalChunksFunc      func() int
	currentChunkSizeFunc func() int
	statsTicker          chan bool
}

type stats struct {
	putAdsSent                     int64
	removeAdsSent                  int64
	cidsProcessed                  int64
	existingCidsProcessed          int64
	cidsExpired                    int64
	delegatedRoutingCallsReceived  int64
	delegatedRoutingCallsProcessed int64
	chunkCacheMisses               int64
	chunksNotFound                 int64
}

func newStatsReporter(totalCidsFunc func() int, totalChunksFunc func() int, currentChunkSizeFunc func() int) *statsReporter {
	return &statsReporter{
		s:                    &stats{},
		totalCidsFunc:        totalCidsFunc,
		totalChunksFunc:      totalChunksFunc,
		currentChunkSizeFunc: currentChunkSizeFunc,
	}
}

func (reporter *statsReporter) incPutAdsSent() {
	reporter.s.putAdsSent++
}

func (reporter *statsReporter) incRemoveAdsSent() {
	reporter.s.removeAdsSent++
}

func (reporter *statsReporter) incCidsProcessed() {
	reporter.s.cidsProcessed++
}

func (reporter *statsReporter) incExistingCidsProcessed() {
	reporter.s.existingCidsProcessed++
}

func (reporter *statsReporter) incCidsExpired() {
	reporter.s.cidsExpired++
}

func (reporter *statsReporter) incDelegatedRoutingCallsReceived() {
	// needs to be threadsafe as it gets called from the webserver handler
	atomic.AddInt64(&reporter.s.delegatedRoutingCallsReceived, 1)
}

func (reporter *statsReporter) incDelegatedRoutingCallsProcessed() {
	reporter.s.delegatedRoutingCallsProcessed++
}

func (reporter *statsReporter) incChunkCacheMisses() {
	reporter.s.chunkCacheMisses++
}

func (reporter *statsReporter) incChunksNotFound() {
	reporter.s.chunksNotFound++
}

func (reporter *statsReporter) start() {
	reporter.statsTicker = make(chan bool)
	ticker := time.NewTicker(statsPrintFrequency)

	go func() {
		for {
			select {
			case <-reporter.statsTicker:
				ticker.Stop()
				return
			case <-ticker.C:
				log.Infof("stats: %+v, totalCids: %d, totalChunks: %d, currentChunkSize: %d", reporter.s, reporter.totalCidsFunc(), reporter.totalChunksFunc(), reporter.currentChunkSizeFunc())
			}
		}
	}()
}

func (reporter *statsReporter) shutdown() {
	reporter.statsTicker <- true
}
