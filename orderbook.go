package orderbook

import (
	"sync"
	"sync/atomic"
)

type Order struct {
	Price     float64
	Quantity  float64
	Timestamp int64
}

type priceLevel struct {
	price    float64
	quantity float64
	next     *priceLevel
	prev     *priceLevel
}

type bnode struct {
	prices   []float64
	levels   []*priceLevel
	children []*bnode
	next     *bnode
	prev     *bnode
	parent   *bnode
	isLeaf   bool
}

type OrderBook struct {
	maxDepth int

	mu      sync.RWMutex
	bids    *bnode
	asks    *bnode
	bidHead *priceLevel
	askHead *priceLevel
	version uint64
}

const btreeDegree = 32

func NewOrderBook(maxDepth int) *OrderBook {
	return &OrderBook{
		maxDepth: maxDepth,
		asks:     &bnode{isLeaf: true},
		bids:     &bnode{isLeaf: true},
	}
}

func (ob *OrderBook) rebuildHeads() {
	ob.bidHead = ob.findHighestBid(ob.bids)
	ob.askHead = ob.findLowestAsk(ob.asks)
}

func (ob *OrderBook) findLowestAsk(n *bnode) *priceLevel {
	if n == nil || len(n.prices) == 0 {
		return nil
	}
	for !n.isLeaf {
		n = n.children[0]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[0]
}

func (ob *OrderBook) findHighestBid(n *bnode) *priceLevel {
	if n == nil || len(n.prices) == 0 {
		return nil
	}
	for !n.isLeaf {
		n = n.children[len(n.children)-1]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[len(n.levels)-1]
}

func (ob *OrderBook) UpdateSnapshot(asks, bids []Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// Bids processing
	if len(bids) > 0 {
		highestBid := bids[0].Price
		for _, bid := range bids {
			if bid.Price > highestBid {
				highestBid = bid.Price
			}
		}

		toRemove := make([]float64, 0)
		curr := ob.bidHead
		for curr != nil && curr.price > highestBid {
			toRemove = append(toRemove, curr.price)
			curr = curr.prev
		}
		for _, p := range toRemove {
			ob.removeBidInternal(p)
		}

		for _, order := range bids {
			if order.Quantity > 0 {
				level := ob.findBid(order.Price)
				if level != nil {
					level.quantity = order.Quantity
				} else {
					ob.insertBid(order.Price, order.Quantity)
				}
			}
		}
	}

	// Asks processing
	if len(asks) > 0 {
		lowestAsk := asks[0].Price
		for _, ask := range asks {
			if ask.Price < lowestAsk {
				lowestAsk = ask.Price
			}
		}

		toRemove := make([]float64, 0)
		curr := ob.askHead
		for curr != nil && curr.price < lowestAsk {
			toRemove = append(toRemove, curr.price)
			curr = curr.next
		}
		for _, p := range toRemove {
			ob.removeAskInternal(p)
		}

		for _, order := range asks {
			if order.Quantity > 0 {
				level := ob.findAsk(order.Price)
				if level != nil {
					level.quantity = order.Quantity
				} else {
					ob.insertAsk(order.Price, order.Quantity)
				}
			}
		}
	}

	ob.rebuildHeads()

	if ob.bidHead != nil && ob.askHead != nil && ob.bidHead.price >= ob.askHead.price {
		toRemove := make([]float64, 0)
		curr := ob.bidHead
		for curr != nil && curr.price >= ob.askHead.price {
			toRemove = append(toRemove, curr.price)
			curr = curr.prev
		}
		for _, p := range toRemove {
			ob.removeBidInternal(p)
		}
		ob.bidHead = ob.findHighestBid(ob.bids)
	}

	atomic.AddUint64(&ob.version, 1)
}

func (ob *OrderBook) UpdateDelta(asks, bids []Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for _, order := range bids {
		ob.updateBid(order.Price, order.Quantity)
	}

	for _, order := range asks {
		ob.updateAsk(order.Price, order.Quantity)
	}

	ob.rebuildHeads()
	atomic.AddUint64(&ob.version, 1)
}

func (ob *OrderBook) GetBestBid() (price float64, quantity float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.bidHead == nil {
		return 0, 0, false
	}
	return ob.bidHead.price, ob.bidHead.quantity, true
}

func (ob *OrderBook) GetBestAsk() (price float64, quantity float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.askHead == nil {
		return 0, 0, false
	}
	return ob.askHead.price, ob.askHead.quantity, true
}

func (ob *OrderBook) GetDepth(levels int) (asks, bids []Order) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	bids = make([]Order, 0, levels)
	asks = make([]Order, 0, levels)

	curr := ob.bidHead
	for curr != nil && len(bids) < levels {
		bids = append(bids, Order{Price: curr.price, Quantity: curr.quantity})
		curr = curr.prev
	}

	curr = ob.askHead
	for curr != nil && len(asks) < levels {
		asks = append(asks, Order{Price: curr.price, Quantity: curr.quantity})
		curr = curr.next
	}

	return asks, bids
}

func (ob *OrderBook) GetMid() (mid float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.bidHead == nil || ob.askHead == nil {
		return 0, false
	}
	return (ob.bidHead.price + ob.askHead.price) / 2, true
}

func (ob *OrderBook) insertBid(price float64, quantity float64) {
	level := ob.searchOrInsert(ob.bids, price, true)
	level.quantity = quantity
}

func (ob *OrderBook) insertAsk(price float64, quantity float64) {
	level := ob.searchOrInsert(ob.asks, price, false)
	level.quantity = quantity
}

func (ob *OrderBook) updateBid(price float64, quantity float64) {
	if quantity == 0 {
		ob.removeBidInternal(price)
	} else {
		level := ob.findBid(price)
		if level != nil {
			level.quantity = quantity
		} else {
			ob.insertBid(price, quantity)
		}
	}
}

func (ob *OrderBook) updateAsk(price float64, quantity float64) {
	if quantity == 0 {
		ob.removeAskInternal(price)
	} else {
		level := ob.findAsk(price)
		if level != nil {
			level.quantity = quantity
		} else {
			ob.insertAsk(price, quantity)
		}
	}
}

// removeBid removes a bid and rebuilds heads (public-facing)
func (ob *OrderBook) removeBid(price float64) {
	ob.removeBidInternal(price)
	ob.rebuildHeads()
}

// removeAsk removes an ask and rebuilds heads (public-facing)
func (ob *OrderBook) removeAsk(price float64) {
	ob.removeAskInternal(price)
	ob.rebuildHeads()
}

// removeBidInternal removes a bid without rebuilding heads
func (ob *OrderBook) removeBidInternal(price float64) {
	ob.removeLevel(ob.bids, price, true)
}

// removeAskInternal removes an ask without rebuilding heads
func (ob *OrderBook) removeAskInternal(price float64) {
	ob.removeLevel(ob.asks, price, false)
}

func (ob *OrderBook) removeLevel(n *bnode, price float64, isBid bool) {
	if n == nil || len(n.prices) == 0 {
		return
	}

	idx := 0
	found := false
	for i, p := range n.prices {
		if p == price {
			idx = i
			found = true
			break
		}
		if (isBid && p > price) || (!isBid && p > price) {
			idx = i
			break
		}
		idx = i + 1
	}

	if n.isLeaf {
		if found {
			level := n.levels[idx]
			if level.prev != nil {
				level.prev.next = level.next
			}
			if level.next != nil {
				level.next.prev = level.prev
			}
			n.prices = append(n.prices[:idx], n.prices[idx+1:]...)
			n.levels = append(n.levels[:idx], n.levels[idx+1:]...)
		}
	} else {
		// For non-leaf nodes, we need to search in the appropriate child
		childIdx := idx
		if found {
			// If found at this level, the actual leaf entry is in the right subtree
			childIdx = idx + 1
		}
		if childIdx < len(n.children) {
			ob.removeLevel(n.children[childIdx], price, isBid)
		}
	}
}

func (ob *OrderBook) findBid(price float64) *priceLevel {
	return ob.search(ob.bids, price, true)
}

func (ob *OrderBook) findAsk(price float64) *priceLevel {
	return ob.search(ob.asks, price, false)
}

func (ob *OrderBook) findOrCreateBid(price float64) *priceLevel {
	return ob.searchOrInsert(ob.bids, price, true)
}

func (ob *OrderBook) findOrCreateAsk(price float64) *priceLevel {
	return ob.searchOrInsert(ob.asks, price, false)
}

func (ob *OrderBook) search(n *bnode, price float64, isBid bool) *priceLevel {
	if n == nil || len(n.prices) == 0 {
		return nil
	}

	idx := 0
	found := false
	for i, p := range n.prices {
		if p == price {
			idx = i
			found = true
			break
		}
		if (isBid && p > price) || (!isBid && p > price) {
			break
		}
		idx = i + 1
	}

	if n.isLeaf {
		if found {
			return n.levels[idx]
		}
		return nil
	}

	if found {
		return n.levels[idx]
	}
	if idx < len(n.children) {
		return ob.search(n.children[idx], price, isBid)
	}
	return nil
}

func (ob *OrderBook) searchOrInsert(n *bnode, price float64, isBid bool) *priceLevel {
	for !n.isLeaf {
		idx := 0
		found := false
		for i, p := range n.prices {
			if p == price {
				return n.levels[i]
			}
			if (isBid && p > price) || (!isBid && p > price) {
				break
			}
			idx = i + 1
		}
		if !found && idx < len(n.children) {
			n = n.children[idx]
		} else {
			break
		}
	}

	if !n.isLeaf {
		return nil
	}

	idx := 0
	for i, p := range n.prices {
		if p == price {
			return n.levels[i]
		}
		if (isBid && p > price) || (!isBid && p > price) {
			break
		}
		idx = i + 1
	}

	level := &priceLevel{price: price}
	ob.insertIntoLeaf(n, idx, price, level, isBid)
	return level
}

func (ob *OrderBook) insertIntoLeaf(n *bnode, idx int, price float64, level *priceLevel, isBid bool) {
	if len(n.prices) >= 2*btreeDegree-1 {
		ob.splitLeaf(n, isBid)

		root := ob.bids
		if !isBid {
			root = ob.asks
		}

		target := root
		for !target.isLeaf {
			childIdx := 0
			for i, p := range target.prices {
				if (isBid && p > price) || (!isBid && p > price) {
					break
				}
				childIdx = i + 1
			}
			if childIdx < len(target.children) {
				target = target.children[childIdx]
			} else {
				break
			}
		}

		idx = 0
		for i, p := range target.prices {
			if (isBid && p > price) || (!isBid && p > price) {
				break
			}
			idx = i + 1
		}
		n = target
	}

	n.prices = append(n.prices[:idx], append([]float64{price}, n.prices[idx:]...)...)
	n.levels = append(n.levels[:idx], append([]*priceLevel{level}, n.levels[idx:]...)...)

	if idx > 0 {
		level.prev = n.levels[idx-1]
		n.levels[idx-1].next = level
	}
	if idx < len(n.levels)-1 {
		level.next = n.levels[idx+1]
		n.levels[idx+1].prev = level
	}
}

func (ob *OrderBook) splitLeaf(n *bnode, isBid bool) {
	if n.parent == nil {
		newRoot := &bnode{isLeaf: false}
		newRoot.children = []*bnode{n}
		n.parent = newRoot
		if isBid {
			ob.bids = newRoot
		} else {
			ob.asks = newRoot
		}
	}

	mid := len(n.prices) / 2
	newNode := &bnode{
		isLeaf: true,
		parent: n.parent,
		prices: append([]float64{}, n.prices[mid:]...),
		levels: append([]*priceLevel{}, n.levels[mid:]...),
		next:   n.next,
		prev:   n,
	}

	if n.next != nil {
		n.next.prev = newNode
	}
	n.next = newNode

	oldPrices := n.prices[:mid]
	oldLevels := n.levels[:mid]
	n.prices = oldPrices
	n.levels = oldLevels

	parent := n.parent
	insertIdx := 0
	for i, child := range parent.children {
		if child == n {
			insertIdx = i
			break
		}
	}

	parent.children = append(parent.children[:insertIdx+1], append([]*bnode{newNode}, parent.children[insertIdx+1:]...)...)
	parent.prices = append(parent.prices[:insertIdx], append([]float64{newNode.prices[0]}, parent.prices[insertIdx:]...)...)
	parent.levels = append(parent.levels[:insertIdx], append([]*priceLevel{newNode.levels[0]}, parent.levels[insertIdx:]...)...)
}
