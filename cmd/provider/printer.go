package main

import (
	"fmt"

	"github.com/ipni/index-provider/cmd/provider/internal"
)

func printVerificationResult(r *internal.VerifyIngestResult) {
	fmt.Println()
	fmt.Println("Verification result:")
	fmt.Printf("  # failed to verify:                   %d\n", r.FailedToVerify)
	fmt.Printf("  # unindexed:                          %d\n", r.Absent)
	fmt.Printf("  # indexed with another provider ID:   %d\n", r.ProviderMismatch)
	fmt.Printf("  # indexed with expected provider ID:  %d\n", r.Present)
	fmt.Println("--------------------------------------------")
	fmt.Printf("total Multihashes checked:              %d\n", r.TotalMhChecked)
	fmt.Println()
	fmt.Printf("sampling probability:                   %.2f\n", samplingProb)
	fmt.Printf("RNG seed:                               %d\n", rngSeed)
	fmt.Println()

	if printUnindexedMhs && len(r.AbsentMhs) != 0 {
		fmt.Println("Un-indexed Multihash(es):")
		for _, mh := range r.AbsentMhs {
			fmt.Printf("  %s\n", mh.B58String())
		}
		fmt.Println()
	}

	if r.TotalMhChecked == 0 {
		fmt.Println("⚠️ Inconclusive; no multihashes were verified.")
	} else if r.PassedVerification() {
		fmt.Println("🎉 Passed verification check.")
	} else {
		fmt.Println("❌ Failed verification check.")
	}

	if len(r.Errs) != 0 {
		fmt.Println("Verification Error(s):")
		for _, err := range r.Errs {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}
}

func printAdStats(stats *internal.AdStats) {
	fmt.Println()
	fmt.Println("Advertisement chain stats:")
	fmt.Printf("  # rm ads:                             %d\n", stats.RmCount)
	fmt.Printf("  # non-rm ads:                         %d\n", stats.NonRmCount)
	fmt.Printf("     # of which had ctx id removed:     %d\n", stats.AdNoLongerProvidedCount)
	fmt.Printf("  # unique context IDs:                 %d\n", stats.UniqueContextIDCount())

	mhStats := stats.NonRmMhStats()
	sum, _ := mhStats.Sum()
	max, _ := mhStats.Max()
	min, _ := mhStats.Min()
	mean, _ := mhStats.Mean()
	std, _ := mhStats.StandardDeviation()
	fmt.Printf("  # max mhs per ad:                     %.0f\n", max)
	fmt.Printf("  # min mhs per ad:                     %.0f\n", min)
	fmt.Printf("  # mean ± std mhs per ad:              %.2f ± %.2f\n", mean, std)

	cStats := stats.NonRmChunkStats()
	cSum, _ := cStats.Sum()
	cMax, _ := cStats.Max()
	cMin, _ := cStats.Min()
	cMean, _ := cStats.Mean()
	cStd, _ := cStats.StandardDeviation()
	fmt.Printf("  # max chunks per ad:                  %.0f\n", cMax)
	fmt.Printf("  # min chunks per ad:                  %.0f\n", cMin)
	fmt.Printf("  # mean ± std chunks per ad:           %.2f ± %.2f\n", cMean, cStd)
	fmt.Println("--------------------------------------------")
	fmt.Printf("total ads:                              %d\n", stats.TotalAdCount())
	fmt.Printf("total mhs:                              %.0f\n", sum)
	fmt.Printf("total chunks:                           %.0f\n", cSum)
	fmt.Println()
}
