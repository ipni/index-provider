package reframe

import (
	"container/list"
	"time"

	"github.com/ipfs/go-cid"
)

type cidQueue struct {
	listNodeByCid map[cid.Cid]*list.Element
	nodesLl       *list.List
}

type cidNode struct {
	Timestamp time.Time
	C         cid.Cid
}

func newCidQueue() *cidQueue {
	return &cidQueue{
		listNodeByCid: make(map[cid.Cid]*list.Element),
		nodesLl:       list.New(),
	}
}

func (cq *cidQueue) recordCidNode(node *cidNode) *list.Element {
	if listElem, ok := cq.listNodeByCid[node.C]; ok {
		listElem.Value.(*cidNode).Timestamp = node.Timestamp
		cq.nodesLl.MoveToFront(listElem)
		return listElem
	} else {
		listElem := cq.nodesLl.PushFront(node)
		cq.listNodeByCid[node.C] = listElem
		return listElem
	}
}

func (cq *cidQueue) removeCidNode(c cid.Cid) {
	if listNode, ok := cq.listNodeByCid[c]; ok {
		cq.nodesLl.Remove(listNode)
		delete(cq.listNodeByCid, c)
	}
}

func (cq *cidQueue) getNodeByCid(c cid.Cid) *list.Element {
	return cq.listNodeByCid[c]
}

func (cq *cidQueue) getTimestampsSnapshot() []*cidNode {
	timestamps := make([]*cidNode, 0, len(cq.listNodeByCid))
	for _, node := range cq.listNodeByCid {
		cNode := node.Value.(*cidNode)
		timestamps = append(timestamps, cNode)
	}
	return timestamps
}
