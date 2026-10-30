package orderbook

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// refBook is a plain map model of OrderBook semantics used to cross-check
// the B+ tree implementation.
type refBook struct {
	maxDepth int
	bids     map[int64]float64
	asks     map[int64]float64
}

func newRefBook(maxDepth int) *refBook {
	return &refBook{maxDepth: maxDepth, bids: map[int64]float64{}, asks: map[int64]float64{}}
}

func sortedKeys(m map[int64]float64, desc bool) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if desc {
			return keys[i] > keys[j]
		}
		return keys[i] < keys[j]
	})
	return keys
}

func (r *refBook) trim() {
	if r.maxDepth <= 0 {
		return
	}
	if keys := sortedKeys(r.bids, true); len(keys) > r.maxDepth {
		for _, k := range keys[r.maxDepth:] {
			delete(r.bids, k)
		}
	}
	if keys := sortedKeys(r.asks, false); len(keys) > r.maxDepth {
		for _, k := range keys[r.maxDepth:] {
			delete(r.asks, k)
		}
	}
}

func (r *refBook) delta(asks, bids []Order) {
	for _, o := range bids {
		if o.Quantity == 0 {
			delete(r.bids, toInt64(o.Price))
		} else {
			r.bids[toInt64(o.Price)] = o.Quantity
		}
	}
	for _, o := range asks {
		if o.Quantity == 0 {
			delete(r.asks, toInt64(o.Price))
		} else {
			r.asks[toInt64(o.Price)] = o.Quantity
		}
	}
	r.trim()
}

func (r *refBook) snapshot(asks, bids []Order) {
	if len(bids) > 0 {
		hi := toInt64(bids[0].Price)
		for _, o := range bids {
			if p := toInt64(o.Price); p > hi {
				hi = p
			}
		}
		lo := hi
		keep := map[int64]bool{}
		for _, o := range bids {
			p := toInt64(o.Price)
			lo = min(lo, p)
			if o.Quantity > 0 {
				keep[p] = true
			}
		}
		for k := range r.bids {
			if k > hi || (k >= lo && !keep[k]) {
				delete(r.bids, k)
			}
		}
		for _, o := range bids {
			if o.Quantity > 0 {
				r.bids[toInt64(o.Price)] = o.Quantity
			}
		}
	}
	if len(asks) > 0 {
		lo := toInt64(asks[0].Price)
		for _, o := range asks {
			if p := toInt64(o.Price); p < lo {
				lo = p
			}
		}
		hi := lo
		keep := map[int64]bool{}
		for _, o := range asks {
			p := toInt64(o.Price)
			hi = max(hi, p)
			if o.Quantity > 0 {
				keep[p] = true
			}
		}
		for k := range r.asks {
			if k < lo || (k <= hi && !keep[k]) {
				delete(r.asks, k)
			}
		}
		for _, o := range asks {
			if o.Quantity > 0 {
				r.asks[toInt64(o.Price)] = o.Quantity
			}
		}
	}
	for {
		b, a := sortedKeys(r.bids, true), sortedKeys(r.asks, false)
		if len(b) == 0 || len(a) == 0 || b[0] < a[0] {
			break
		}
		for _, k := range b {
			if k >= a[0] {
				delete(r.bids, k)
			}
		}
		b = sortedKeys(r.bids, true)
		if len(b) > 0 && b[0] >= a[0] {
			for _, k := range a {
				if k <= b[0] {
					delete(r.asks, k)
				}
			}
		}
	}
	r.trim()
}

// checkTree verifies B+ tree invariants of one side and returns its leaf keys
// in ascending order.
func checkTree(t *testing.T, root *bnode) []int64 {
	t.Helper()
	var keys []int64
	var walk func(n *bnode, lo, hi int64, depth int) int
	walk = func(n *bnode, lo, hi int64, depth int) int {
		for i, p := range n.prices {
			if p < lo || p >= hi {
				t.Fatalf("key %d outside separator range [%d,%d)", p, lo, hi)
			}
			if i > 0 && n.prices[i-1] >= p {
				t.Fatalf("keys not strictly ascending in node: %v", n.prices)
			}
		}
		if n.isLeaf {
			if len(n.levels) != len(n.prices) {
				t.Fatalf("leaf has %d prices but %d levels", len(n.prices), len(n.levels))
			}
			for i, l := range n.levels {
				if l.price != n.prices[i] {
					t.Fatalf("leaf level price %d != key %d", l.price, n.prices[i])
				}
			}
			keys = append(keys, n.prices...)
			return depth
		}
		if len(n.children) != len(n.prices)+1 {
			t.Fatalf("internal node has %d keys and %d children", len(n.prices), len(n.children))
		}
		leafDepth := -1
		for i, c := range n.children {
			clo, chi := lo, hi
			if i > 0 {
				clo = n.prices[i-1]
			}
			if i < len(n.prices) {
				chi = n.prices[i]
			}
			d := walk(c, clo, chi, depth+1)
			if leafDepth >= 0 && d != leafDepth {
				t.Fatalf("leaves at different depths")
			}
			leafDepth = d
		}
		return leafDepth
	}
	walk(root, -1<<62, 1<<62, 0)
	return keys
}

func checkAgainstRef(t *testing.T, ob *OrderBook, ref *refBook, step int) {
	t.Helper()
	bidKeys := checkTree(t, ob.bids)
	askKeys := checkTree(t, ob.asks)

	wantBids := sortedKeys(ref.bids, false)
	wantAsks := sortedKeys(ref.asks, false)
	if fmt.Sprint(bidKeys) != fmt.Sprint(wantBids) {
		t.Fatalf("step %d: bid tree keys\n got %v\nwant %v", step, bidKeys, wantBids)
	}
	if fmt.Sprint(askKeys) != fmt.Sprint(wantAsks) {
		t.Fatalf("step %d: ask tree keys\n got %v\nwant %v", step, askKeys, wantAsks)
	}

	// The linked list and depth counters must agree with the tree.
	asks, bids := ob.GetDepth(len(wantBids) + len(wantAsks) + 10)
	if len(bids) != len(wantBids) || len(asks) != len(wantAsks) {
		t.Fatalf("step %d: GetDepth returned %d bids/%d asks, want %d/%d",
			step, len(bids), len(asks), len(wantBids), len(wantAsks))
	}
	for i, o := range bids {
		p := wantBids[len(wantBids)-1-i]
		if toInt64(o.Price) != p || o.Quantity != ref.bids[p] {
			t.Fatalf("step %d: bid %d = %v, want %d:%v", step, i, o, p, ref.bids[p])
		}
	}
	for i, o := range asks {
		p := wantAsks[i]
		if toInt64(o.Price) != p || o.Quantity != ref.asks[p] {
			t.Fatalf("step %d: ask %d = %v, want %d:%v", step, i, o, p, ref.asks[p])
		}
	}
	if ob.bidDepth != len(wantBids) || ob.askDepth != len(wantAsks) {
		t.Fatalf("step %d: depth counters %d/%d, want %d/%d",
			step, ob.bidDepth, ob.askDepth, len(wantBids), len(wantAsks))
	}
}

// genSide produces levels around mid on one side, mimicking a diff stream:
// most updates sit near the touch, some are removals of existing levels.
func genSide(rng *rand.Rand, mid int, isBid bool, n, spread, removePct int) []Order {
	out := make([]Order, 0, n)
	for i := 0; i < n; i++ {
		off := 1 + int(rng.ExpFloat64()*float64(spread))
		p := mid + off
		if isBid {
			p = mid - off
		}
		q := 0.0
		if rng.Intn(100) >= removePct {
			q = float64(1+rng.Intn(1000)) / 100
		}
		out = append(out, Order{Price: float64(p) / 10, Quantity: q})
	}
	return out
}

func runRandomized(t *testing.T, seed int64, maxDepth, steps, spread int) {
	rng := rand.New(rand.NewSource(seed))
	ob := NewOrderBook(maxDepth)
	ref := newRefBook(maxDepth)
	mid := 100000

	snap := func() {
		n := maxDepth
		if n <= 0 {
			n = 300
		}
		asks := genSide(rng, mid, false, n, spread, 0)
		bids := genSide(rng, mid, true, n, spread, 0)
		ob.UpdateSnapshot(asks, bids)
		ref.snapshot(asks, bids)
	}
	snap()
	checkAgainstRef(t, ob, ref, 0)

	for step := 1; step <= steps; step++ {
		mid += rng.Intn(21) - 10
		if rng.Intn(500) == 0 {
			snap()
		} else {
			asks := genSide(rng, mid, false, 1+rng.Intn(30), spread, 40)
			bids := genSide(rng, mid, true, 1+rng.Intn(30), spread, 40)
			// The stream removes levels that drifted to the other side of mid.
			for p := range ref.bids {
				if p >= toInt64(float64(mid)/10) && rng.Intn(2) == 0 {
					bids = append(bids, Order{Price: toFloat64(p), Quantity: 0})
				}
			}
			for p := range ref.asks {
				if p <= toInt64(float64(mid)/10) && rng.Intn(2) == 0 {
					asks = append(asks, Order{Price: toFloat64(p), Quantity: 0})
				}
			}
			ob.UpdateDelta(asks, bids)
			ref.delta(asks, bids)
		}
		checkAgainstRef(t, ob, ref, step)
	}
}

// TestRandomizedAgainstMap feeds random snapshot/delta streams with depth
// trimming and compares every step against a map book.
func TestRandomizedAgainstMap(t *testing.T) {
	cases := []struct {
		maxDepth, spread int
	}{
		{100, 40},
		{100, 150},
		{50, 30},
		{20, 10},
		{500, 200},
		{0, 300},
		{0, 3000},
		{2000, 2000},
	}
	if testing.Short() {
		cases = cases[:4]
	}
	for _, c := range cases {
		for seed := int64(1); seed <= 20; seed++ {
			t.Run(fmt.Sprintf("depth%d_spread%d_seed%d", c.maxDepth, c.spread, seed), func(t *testing.T) {
				runRandomized(t, seed, c.maxDepth, 3000, c.spread)
			})
		}
	}
}

// TestSnapshotRemovesLevelsMissingFromSnapshot covers a resync: deltas
// received while the snapshot was fetched are dropped, so the book still holds
// levels the exchange removed meanwhile. The snapshot must drop them.
func TestSnapshotRemovesLevelsMissingFromSnapshot(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 101, Quantity: 1}, {Price: 102, Quantity: 1}, {Price: 103, Quantity: 1}, {Price: 110, Quantity: 1}},
		[]Order{{Price: 100, Quantity: 1}, {Price: 99, Quantity: 1}, {Price: 98, Quantity: 1}, {Price: 90, Quantity: 1}},
	)

	// The market moved up while deltas were skipped: asks 101 and 102 and
	// bid 99 are gone. Levels beyond the snapshot range (bid 90, ask 110)
	// are not covered by it and stay.
	ob.UpdateSnapshot(
		[]Order{{Price: 103, Quantity: 2}, {Price: 104, Quantity: 1}},
		[]Order{{Price: 102.5, Quantity: 1}, {Price: 100, Quantity: 1}, {Price: 98, Quantity: 0}, {Price: 97, Quantity: 1}},
	)

	asks, bids := ob.GetDepth(10)
	wantAsks := []Order{{Price: 103, Quantity: 2}, {Price: 104, Quantity: 1}, {Price: 110, Quantity: 1}}
	wantBids := []Order{{Price: 102.5, Quantity: 1}, {Price: 100, Quantity: 1}, {Price: 97, Quantity: 1}, {Price: 90, Quantity: 1}}
	if fmt.Sprint(asks) != fmt.Sprint(wantAsks) {
		t.Errorf("asks = %v, want %v", asks, wantAsks)
	}
	if fmt.Sprint(bids) != fmt.Sprint(wantBids) {
		t.Errorf("bids = %v, want %v", bids, wantBids)
	}

	// The stale ask at 101 must not be left under the new bids.
	bid, _, _ := ob.GetBestBid()
	ask, _, _ := ob.GetBestAsk()
	if bid >= ask {
		t.Fatalf("book crossed: bid %v >= ask %v", bid, ask)
	}
	if ob.bidDepth != 4 || ob.askDepth != 3 {
		t.Fatalf("depth counters %d/%d, want 4/3", ob.bidDepth, ob.askDepth)
	}
}

// TestResyncStreamNoStaleLevels mimics a Binance diff stream on a depth
// limited book: deltas are applied to an unlimited truth book, and the
// OrderBook periodically misses a run of them before a top-N snapshot
// resyncs it. Every level the OrderBook holds must exist in the truth book.
func TestResyncStreamNoStaleLevels(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		const depth = 100
		ob := NewOrderBook(depth)
		truth := newRefBook(0)
		mid := 100000

		topN := func(m map[int64]float64, desc bool) []Order {
			keys := sortedKeys(m, desc)
			if len(keys) > depth {
				keys = keys[:depth]
			}
			out := make([]Order, len(keys))
			for i, k := range keys {
				out[i] = Order{Price: toFloat64(k), Quantity: m[k]}
			}
			return out
		}
		// Seed the truth book deeper than the OrderBook keeps.
		truth.snapshot(genSide(rng, mid, false, 1000, 300, 0), genSide(rng, mid, true, 1000, 300, 0))
		ob.UpdateSnapshot(topN(truth.asks, false), topN(truth.bids, true))

		skipping := 0
		for step := 1; step <= 4000; step++ {
			mid += rng.Intn(21) - 10
			asks := genSide(rng, mid, false, 1+rng.Intn(30), 40, 40)
			bids := genSide(rng, mid, true, 1+rng.Intn(30), 40, 40)
			for p := range truth.bids {
				if p >= toInt64(float64(mid)/10) {
					bids = append(bids, Order{Price: toFloat64(p), Quantity: 0})
				}
			}
			for p := range truth.asks {
				if p <= toInt64(float64(mid)/10) {
					asks = append(asks, Order{Price: toFloat64(p), Quantity: 0})
				}
			}
			truth.delta(asks, bids)

			switch {
			case skipping > 1:
				skipping--
			case skipping == 1:
				skipping = 0
				ob.UpdateSnapshot(topN(truth.asks, false), topN(truth.bids, true))
			case rng.Intn(200) == 0:
				skipping = 1 + rng.Intn(50)
			default:
				ob.UpdateDelta(asks, bids)
			}
			if skipping > 0 {
				continue
			}

			gotAsks, gotBids := ob.GetDepth(depth)
			for _, o := range gotAsks {
				if _, ok := truth.asks[toInt64(o.Price)]; !ok {
					t.Fatalf("seed %d step %d: stale ask %v", seed, step, o.Price)
				}
			}
			for _, o := range gotBids {
				if _, ok := truth.bids[toInt64(o.Price)]; !ok {
					t.Fatalf("seed %d step %d: stale bid %v", seed, step, o.Price)
				}
			}
			if len(gotAsks) > 0 && len(gotBids) > 0 && gotBids[0].Price >= gotAsks[0].Price {
				t.Fatalf("seed %d step %d: crossed, bid %v >= ask %v", seed, step, gotBids[0].Price, gotAsks[0].Price)
			}
		}
	}
}
