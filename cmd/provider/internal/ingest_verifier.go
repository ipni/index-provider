package internal

import (
	"context"
	"fmt"

	httpfinderclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/multiformats/go-multihash"
)

const verifyChunkSize = 4096

type VerifyIngestResult struct {
	TotalMhChecked   int
	ProviderMismatch int
	Present          int
	Absent           int
	FailedToVerify   int
	Errs             []error
	AbsentMhs        []multihash.Multihash
}

func (r *VerifyIngestResult) PassedVerification() bool {
	return r.Present == r.TotalMhChecked
}

func (r *VerifyIngestResult) Add(other *VerifyIngestResult) {
	r.TotalMhChecked += other.TotalMhChecked
	r.ProviderMismatch += other.ProviderMismatch
	r.Present += other.Present
	r.Absent += other.Absent
	r.FailedToVerify += other.FailedToVerify
	r.Errs = append(r.Errs, other.Errs...)
	r.AbsentMhs = append(r.AbsentMhs, other.AbsentMhs...)
}

func VerifyIngestFromMhs(finder *httpfinderclient.Client, wantProvID string, mhs []multihash.Multihash) (*VerifyIngestResult, error) {
	aggResult := &VerifyIngestResult{}
	for len(mhs) >= verifyChunkSize {
		result, err := doVerifyIngest(finder, wantProvID, mhs[:verifyChunkSize])
		if err != nil {
			return nil, err
		}
		aggResult.Add(result)
		mhs = mhs[verifyChunkSize:]
	}
	if len(mhs) != 0 {
		result, err := doVerifyIngest(finder, wantProvID, mhs)
		if err != nil {
			return nil, err
		}
		aggResult.Add(result)
	}
	return aggResult, nil
}

func doVerifyIngest(finder *httpfinderclient.Client, wantProvID string, mhs []multihash.Multihash) (*VerifyIngestResult, error) {
	result := &VerifyIngestResult{}
	mhsCount := len(mhs)
	result.TotalMhChecked = mhsCount
	response, err := finder.FindBatch(context.Background(), mhs)
	if err != nil {
		result.FailedToVerify = mhsCount
		err = fmt.Errorf("failed to connect to indexer: %w", err)
		result.Errs = append(result.Errs, err)
		return result, nil
	}

	if len(response.MultihashResults) == 0 {
		result.Absent = mhsCount
		return result, nil
	}

	resultsByMh := make(map[string]model.MultihashResult, len(response.MultihashResults))
	for _, mr := range response.MultihashResults {
		resultsByMh[mr.Multihash.String()] = mr
	}

	for _, mh := range mhs {
		gotResult, ok := resultsByMh[mh.String()]
		if !ok || len(gotResult.ProviderResults) == 0 {
			result.Absent++
			result.AbsentMhs = append(result.AbsentMhs, mh)
			continue
		}

		var provMatched bool
		for _, p := range gotResult.ProviderResults {
			gotProvID := p.Provider.ID.String()
			if gotProvID == wantProvID {
				result.Present++
				provMatched = true
				break
			}
		}
		if !provMatched {
			result.ProviderMismatch++
		}
	}
	return result, nil
}
