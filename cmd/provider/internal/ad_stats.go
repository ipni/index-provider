package internal

import (
	"errors"

	"github.com/ipfs/go-datastore"
	"github.com/montanaflynn/stats"
	"github.com/multiformats/go-multihash"
)

type (
	Sampler func() bool

	AdStats struct {
		sampler                 Sampler
		NonRmCount              int
		RmCount                 int
		AdNoLongerProvidedCount int

		ctxIDRm map[string]bool
		samples []*AdSample

		mhCountDist    []interface{}
		chunkCountDist []interface{}
	}

	AdSample struct {
		IsRemove         bool
		NoLongerProvided bool
		ctxID            string
		PartiallySynced  bool
		SyncErr          error
		ChunkCount       int
		MhCount          int
		MhSample         []multihash.Multihash
	}
)

func NewAdStats(s Sampler) *AdStats {
	if s == nil {
		s = func() bool { return true }
	}
	return &AdStats{
		ctxIDRm: make(map[string]bool),
		sampler: s,
	}
}

func (a *AdStats) Sample(ad *Advertisement) *AdSample {
	sample := &AdSample{
		IsRemove: ad.IsRemove,
		ctxID:    string(ad.ContextID),
	}

	if sample.IsRemove {
		a.RmCount++
		a.ctxIDRm[sample.ctxID] = true

		a.samples = append(a.samples, sample)
		return sample
	}

	a.NonRmCount++
	removed, seen := a.ctxIDRm[sample.ctxID]
	if seen && removed {
		sample.NoLongerProvided = true
		a.AdNoLongerProvidedCount++

		a.samples = append(a.samples, sample)
		return sample
	}

	a.ctxIDRm[sample.ctxID] = false
	if !ad.HasEntries() {
		a.samples = append(a.samples, sample)
		return sample
	}

	allMhs, err := ad.Entries.Drain()
	if err != nil {
		sample.PartiallySynced = true
		// Most likely caused by entries recursion limit reached.
		if errors.Is(err, datastore.ErrNotFound) {
			err = errors.New("recursion limit reached")
		}
		sample.SyncErr = err
	}
	sample.MhCount = len(allMhs)
	sample.ChunkCount = ad.Entries.ChunkCount()

	for _, mh := range allMhs {
		if a.sampler() {
			sample.MhSample = append(sample.MhSample, mh)
		}
	}
	a.samples = append(a.samples, sample)

	a.mhCountDist = append(a.mhCountDist, sample.MhCount)
	a.chunkCountDist = append(a.chunkCountDist, sample.ChunkCount)
	return sample
}

func (a *AdStats) TotalAdCount() int {
	return a.NonRmCount + a.RmCount
}

func (a *AdStats) UniqueContextIDCount() int {
	return len(a.ctxIDRm)
}

func (a *AdStats) NonRmMhStats() stats.Float64Data {
	return stats.LoadRawData(a.mhCountDist)
}

func (a *AdStats) NonRmChunkStats() stats.Float64Data {
	return stats.LoadRawData(a.chunkCountDist)
}
