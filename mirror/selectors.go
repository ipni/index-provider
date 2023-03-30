package mirror

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/dagsync"
)

var selectors struct {
	entriesWithLimit      func(limit selector.RecursionLimit) ipld.Node
	adsWithRecursionLimit func(selector.RecursionLimit) ipld.Node
	adsWithStopAt         func(selector.RecursionLimit, ipld.Link) ipld.Node
}

func init() {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSequenceBuilder := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})
	selectors.entriesWithLimit = func(limit selector.RecursionLimit) ipld.Node {
		return ssb.ExploreRecursive(limit,
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Next", ssb.ExploreRecursiveEdge())
			})).Node()
	}
	selectors.adsWithRecursionLimit = func(limit selector.RecursionLimit) ipld.Node {
		return ssb.ExploreRecursive(limit, adSequenceBuilder).Node()
	}
	selectors.adsWithStopAt = func(limit selector.RecursionLimit, stop ipld.Link) ipld.Node {
		return dagsync.ExploreRecursiveWithStopNode(limit, adSequenceBuilder.Node(), stop)
	}
}
