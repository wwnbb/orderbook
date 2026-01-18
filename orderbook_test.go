package orderbook

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func printAsksBids(asks, bids []Order) {
	fmt.Println("=== Order Book ===")

	fmt.Println("Asks (highest -> lowest):")
	for i := len(asks) - 1; i >= 0; i-- {
		ask := asks[i]
		fmt.Printf("  %10.4f | qty: %10.4f\n", ask.Price, ask.Quantity)
	}

	fmt.Println("----------------------------")

	fmt.Println("Bids (highest -> lowest):")
	for _, bid := range bids {
		fmt.Printf("  %10.4f | qty: %10.4f\n", bid.Price, bid.Quantity)
	}

	fmt.Println("============================")
}

func TestRemove(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 101.0, Quantity: 10.0}}, // asks
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids
	)

	price, qty, ok := ob.GetBestBid()
	if !ok || price != 100.0 || qty != 10.0 {
		t.Fatalf("expected price=100, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
	price, qty, ok = ob.GetBestAsk()
	if !ok || price != 101.0 || qty != 10.0 {
		t.Fatalf("expected price=101, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
	ob.removeAsk(101.0)
	ob.removeBid(100.0)

	_, _, ok = ob.GetBestBid()
	if ok {
		t.Error("Bids should be empty")
	}
	_, _, ok = ob.GetBestAsk()
	if ok {
		t.Error("Asks should be empty")
	}
}

func TestUpdateByDelta(t *testing.T) {
	ob := NewOrderBook(3)
	ob.UpdateSnapshot(
		[]Order{
			{Price: 100.0, Quantity: 10.0},
			{Price: 99.0, Quantity: 5.0},
			{Price: 98.0, Quantity: 15.0},
		},
		[]Order{
			{Price: 97.0, Quantity: 10.0},
			{Price: 95.0, Quantity: 5.0},
			{Price: 96.0, Quantity: 5.0},
		},
	)
	asks, bids := ob.GetDepth(3)
	printAsksBids(asks, bids)
	ob.UpdateDelta(
		[]Order{},
		[]Order{
			{Price: 97.0, Quantity: 0},
			{Price: 94.0, Quantity: 12.0},
		},
	)
	asks, bids = ob.GetDepth(3)
	printAsksBids(asks, bids)
	ob.UpdateSnapshot(
		[]Order{{Price: 95.0, Quantity: 8.0}},
		[]Order{{Price: 94.0, Quantity: 12.0}},
	)
	asks, bids = ob.GetDepth(3)
	printAsksBids(asks, bids)

	ob.UpdateSnapshot(
		[]Order{
			{Price: 100.0, Quantity: 10.0},
			{Price: 99.0, Quantity: 5.0},
			{Price: 98.0, Quantity: 15.0},
		},
		[]Order{
			{Price: 97.0, Quantity: 10.0},
			{Price: 95.0, Quantity: 5.0},
			{Price: 96.0, Quantity: 5.0},
		},
	)
	asks, bids = ob.GetDepth(3)
	printAsksBids(asks, bids)
}

func TestInsertAndRetrieveBid(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{},                               // asks
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids
	)

	price, qty, ok := ob.GetBestBid()
	if !ok || price != 100.0 || qty != 10.0 {
		t.Fatalf("expected price=100, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
}

func TestInsertAndRetrieveAsk(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}}, // asks
		[]Order{},                               // bids
	)

	price, qty, ok := ob.GetBestAsk()
	if !ok || price != 100.0 || qty != 10.0 {
		t.Fatalf("expected price=100, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
}

func TestMultipleBidsOrdered(t *testing.T) {
	ob := NewOrderBook(100)
	bids := []Order{
		{Price: 100.0, Quantity: 10.0},
		{Price: 101.0, Quantity: 20.0},
		{Price: 99.0, Quantity: 30.0},
	}
	ob.UpdateSnapshot([]Order{}, bids)

	price, qty, ok := ob.GetBestBid()
	if !ok || price != 101.0 || qty != 20.0 {
		t.Fatalf("expected best bid price=101, qty=20, got price=%v, qty=%v", price, qty)
	}

	_, bidsDepth := ob.GetDepth(10)
	if len(bidsDepth) != 3 {
		t.Fatalf("expected 3 bids, got %d", len(bidsDepth))
	}

	if bidsDepth[0].Price != 101.0 || bidsDepth[1].Price != 100.0 || bidsDepth[2].Price != 99.0 {
		t.Fatalf("bids not ordered correctly: %v", bidsDepth)
	}
}

func TestMultipleAsksOrdered(t *testing.T) {
	ob := NewOrderBook(100)
	asks := []Order{
		{Price: 100.0, Quantity: 10.0},
		{Price: 99.0, Quantity: 20.0},
		{Price: 101.0, Quantity: 30.0},
	}
	ob.UpdateSnapshot(asks, []Order{})

	price, qty, ok := ob.GetBestAsk()
	if !ok || price != 99.0 || qty != 20.0 {
		t.Fatalf("expected best ask price=99, qty=20, got price=%v, qty=%v", price, qty)
	}

	asksDepth, _ := ob.GetDepth(10)
	if len(asksDepth) != 3 {
		t.Fatalf("expected 3 asks, got %d", len(asksDepth))
	}

	if asksDepth[0].Price != 99.0 || asksDepth[1].Price != 100.0 || asksDepth[2].Price != 101.0 {
		t.Fatalf("asks not ordered correctly: %v", asksDepth)
	}
}

func TestDeltaUpdateAddLevel(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{},                               // asks
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids
	)

	ob.UpdateDelta(
		[]Order{},                               // asks
		[]Order{{Price: 101.0, Quantity: 20.0}}, // bids
	)

	_, bidsDepth := ob.GetDepth(10)
	if len(bidsDepth) != 2 {
		t.Fatalf("expected 2 bids, got %d", len(bidsDepth))
	}
	if bidsDepth[0].Price != 101.0 {
		t.Fatalf("expected best bid 101, got %v", bidsDepth[0].Price)
	}
}

func TestDeltaUpdateModifyLevel(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{},                               // asks
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids
	)

	ob.UpdateDelta(
		[]Order{},                               // asks
		[]Order{{Price: 100.0, Quantity: 15.0}}, // bids
	)

	_, qty, ok := ob.GetBestBid()
	if !ok || qty != 15.0 {
		t.Fatalf("expected qty=15, got qty=%v", qty)
	}
}

func TestDeltaUpdateRemoveLevel(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{}, // asks
		[]Order{{Price: 100.0, Quantity: 10.0}, {Price: 101.0, Quantity: 20.0}}, // bids
	)

	ob.UpdateDelta(
		[]Order{},                            // asks
		[]Order{{Price: 100.0, Quantity: 0}}, // bids - remove level
	)

	_, bidsDepth := ob.GetDepth(10)
	if len(bidsDepth) != 1 {
		t.Fatalf("expected 1 bid, got %d", len(bidsDepth))
	}
	if bidsDepth[0].Price != 101.0 {
		t.Fatalf("expected remaining bid 101, got %v", bidsDepth[0].Price)
	}
}

func TestGetMid(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 102.0, Quantity: 10.0}}, // asks (higher price)
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids (lower price)
	)

	mid, ok := ob.GetMid()
	if !ok || mid != 101.0 {
		t.Fatalf("expected mid=101, got mid=%v", mid)
	}
}

func TestGetMidNoLiquidity(t *testing.T) {
	ob := NewOrderBook(100)
	_, ok := ob.GetMid()
	if ok {
		t.Fatal("expected no mid when book is empty")
	}
}

func TestEmptyBook(t *testing.T) {
	ob := NewOrderBook(100)
	_, _, ok := ob.GetBestBid()
	if ok {
		t.Fatal("expected no best bid in empty book")
	}

	_, _, ok = ob.GetBestAsk()
	if ok {
		t.Fatal("expected no best ask in empty book")
	}
}

func TestDepthLimiting(t *testing.T) {
	ob := NewOrderBook(100)
	bids := make([]Order, 50)
	for i := 0; i < 50; i++ {
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: float64(i + 1)}
	}
	ob.UpdateSnapshot([]Order{}, bids)

	_, bidsDepth := ob.GetDepth(10)
	if len(bidsDepth) != 10 {
		t.Fatalf("expected 10 bids, got %d", len(bidsDepth))
	}
}

func TestConcurrentReadsWithSnapshot(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 102.0, Quantity: 10.0}}, // asks
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids
	)

	var wg sync.WaitGroup
	readCount := 100

	for i := 0; i < readCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, ok := ob.GetBestBid()
			if !ok {
				t.Error("bid not found during concurrent read")
			}
			_, _, ok = ob.GetBestAsk()
			if !ok {
				t.Error("ask not found during concurrent read")
			}
			_, ok = ob.GetMid()
			if !ok {
				t.Error("mid not found during concurrent read")
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentReadsWithDeltas(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 102.0, Quantity: 10.0}}, // asks
		[]Order{{Price: 100.0, Quantity: 10.0}}, // bids
	)

	var wg sync.WaitGroup
	iterations := 50

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ob.UpdateDelta(
				[]Order{{Price: 100.0 + float64(idx), Quantity: float64(idx + 1)}},
				[]Order{{Price: 102.0 + float64(idx), Quantity: float64(idx + 1)}},
			)
		}(i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			ob.GetBestBid()
			ob.GetBestAsk()
			ob.GetMid()
			ob.GetDepth(10)
		}()
	}

	wg.Wait()
}

func TestVersionIncrement(t *testing.T) {
	ob := NewOrderBook(100)
	v1 := atomic.LoadUint64(&ob.version)

	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{},
	)
	v2 := atomic.LoadUint64(&ob.version)

	if v2 != v1+1 {
		t.Fatalf("expected version increment, got v1=%d, v2=%d", v1, v2)
	}

	ob.UpdateDelta(
		[]Order{{Price: 101.0, Quantity: 20.0}},
		[]Order{},
	)
	v3 := atomic.LoadUint64(&ob.version)

	if v3 != v2+1 {
		t.Fatalf("expected version increment, got v2=%d, v3=%d", v2, v3)
	}
}

func TestLargeSnapshot(t *testing.T) {
	ob := NewOrderBook(1000)
	numLevels := 1000

	bids := make([]Order, numLevels)
	asks := make([]Order, numLevels)

	for i := 0; i < numLevels; i++ {
		bids[i] = Order{Price: 1000.0 + float64(i)*0.01, Quantity: float64(i + 1)}
		asks[i] = Order{Price: 2000.0 + float64(i)*0.01, Quantity: float64(i + 1)}
	}

	ob.UpdateSnapshot(asks, bids)

	asksDepth, bidsDepth := ob.GetDepth(numLevels)
	if len(bidsDepth) != numLevels || len(asksDepth) != numLevels {
		t.Fatalf("expected full depth, got %d bids, %d asks", len(bidsDepth), len(asksDepth))
	}
}

func TestOrderBookSmallSnapshotUpdate(t *testing.T) {
	ob := NewOrderBook(1000)
	numLevels := 1000

	asks := make([]Order, numLevels)
	bids := make([]Order, numLevels)

	for i := 0; i < numLevels; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 1}
		bids[i] = Order{Price: 1999.0 - float64(i), Quantity: 1}
	}
	ob.UpdateSnapshot(asks, bids)
	asksDepth, bidsDepth := ob.GetDepth(numLevels)
	if len(bidsDepth) != numLevels || len(asksDepth) != numLevels {
		t.Fatalf("expected full depth, got %d bids, %d asks", len(bidsDepth), len(asksDepth))
	}
	asksOut, bidsOut := ob.GetDepth(1)
	fmt.Printf("Best Bid: Price=%f, Quantity=%f\n", bidsOut[0].Price, bidsOut[0].Quantity)
	fmt.Printf("Best Ask: Price=%f, Quantity=%f\n", asksOut[0].Price, asksOut[0].Quantity)

	newBids := []Order{{Price: 1995.0, Quantity: 1}}
	newAsks := []Order{{Price: 1996.0, Quantity: 1}}
	ob.UpdateSnapshot(newAsks, newBids)

	asksOut, bidsOut = ob.GetDepth(1)
	fmt.Printf("Best Bid: Price=%f, Quantity=%f\n", bidsOut[0].Price, bidsOut[0].Quantity)
	fmt.Printf("Best Ask: Price=%f, Quantity=%f\n", asksOut[0].Price, asksOut[0].Quantity)
}

func BenchmarkGetBestBid(b *testing.B) {
	ob := NewOrderBook(100)
	bids := make([]Order, 100)
	for i := 0; i < 100; i++ {
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot([]Order{}, bids)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.GetBestBid()
	}
}

func BenchmarkGetDepth(b *testing.B) {
	ob := NewOrderBook(100)
	bids := make([]Order, 100)
	asks := make([]Order, 100)
	for i := 0; i < 100; i++ {
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
		asks[i] = Order{Price: 200.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(asks, bids)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.GetDepth(10)
	}
}

func BenchmarkUpdateDelta(b *testing.B) {
	ob := NewOrderBook(1000)
	asks := make([]Order, 1000)
	bids := make([]Order, 1000)
	for i := 0; i < 1000; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 10.0}
		bids[i] = Order{Price: 1999.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(
		[]Order{
			{Price: 2301.0, Quantity: 11.0},
			{Price: 2983.0, Quantity: 16.0},
		}, // asks
		[]Order{
			{Price: 1070.0, Quantity: 10.0},
			{Price: 1060.0, Quantity: 0.0},
			{Price: 1070.0, Quantity: 99.0},
		}, // bids
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.UpdateDelta(
			[]Order{{Price: 100.0 + float64(i%100), Quantity: float64(i%1000 + 1)}},
			[]Order{{Price: 102.0 + float64(i%100), Quantity: float64(i%1000 + 1)}},
		)
	}
}

func BenchmarkConcurrentReads(b *testing.B) {
	ob := NewOrderBook(100)
	bids := make([]Order, 100)
	asks := make([]Order, 100)
	for i := 0; i < 100; i++ {
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
		asks[i] = Order{Price: 200.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(asks, bids)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ob.GetBestBid()
			ob.GetBestAsk()
			ob.GetMid()
		}
	})
}

func BenchmarkConcurrentReadsAndWrites(b *testing.B) {
	ob := NewOrderBook(100)
	bids := make([]Order, 100)
	asks := make([]Order, 100)
	for i := 0; i < 100; i++ {
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
		asks[i] = Order{Price: 200.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(asks, bids)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				ob.UpdateDelta(
					[]Order{{Price: 100.0 + float64(rand.Intn(100)), Quantity: float64(rand.Intn(100) + 1)}},
					[]Order{{Price: 102.0 + float64(rand.Intn(100)), Quantity: float64(rand.Intn(100) + 1)}},
				)
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ob.GetBestBid()
			ob.GetBestAsk()
		}
	})

	close(stop)
	wg.Wait()
}

func TestDeltaRemovesAllHeadLevels(t *testing.T) {
	// Test removing all levels from the head side via deltas
	ob := NewOrderBook(100)

	// Create orderbook with multiple leaves (> 63 elements triggers split)
	asks := make([]Order, 100)
	bids := make([]Order, 100)
	for i := 0; i < 100; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 1}
		bids[i] = Order{Price: 1000.0 + float64(i), Quantity: 1}
	}
	ob.UpdateSnapshot(asks, bids)

	askPrice, _, ok := ob.GetBestAsk()
	if !ok || askPrice != 2000.0 {
		t.Fatalf("initial best ask should be 2000, got %v, ok=%v", askPrice, ok)
	}
	bidPrice, _, ok := ob.GetBestBid()
	if !ok || bidPrice != 1099.0 {
		t.Fatalf("initial best bid should be 1099, got %v, ok=%v", bidPrice, ok)
	}

	// Remove first 50 asks (head levels) via delta
	deltaAsks := make([]Order, 50)
	for i := 0; i < 50; i++ {
		deltaAsks[i] = Order{Price: 2000.0 + float64(i), Quantity: 0}
	}
	ob.UpdateDelta(deltaAsks, []Order{})

	// Best ask should now be 2050
	askPrice, _, ok = ob.GetBestAsk()
	if !ok {
		t.Fatalf("asks should not be empty after removing 50 levels, got ok=false")
	}
	if askPrice != 2050.0 {
		t.Fatalf("best ask should be 2050, got %v", askPrice)
	}

	// Remove last 50 bids (head levels) via delta
	deltaBids := make([]Order, 50)
	for i := 0; i < 50; i++ {
		deltaBids[i] = Order{Price: 1099.0 - float64(i), Quantity: 0}
	}
	ob.UpdateDelta([]Order{}, deltaBids)

	bidPrice, _, ok = ob.GetBestBid()
	if !ok {
		t.Fatalf("bids should not be empty after removing 50 levels, got ok=false")
	}
	if bidPrice != 1049.0 {
		t.Fatalf("best bid should be 1049, got %v", bidPrice)
	}

	asksDepth, bidsDepth := ob.GetDepth(100)
	if len(asksDepth) != 50 {
		t.Fatalf("expected 50 asks remaining, got %d", len(asksDepth))
	}
	if len(bidsDepth) != 50 {
		t.Fatalf("expected 50 bids remaining, got %d", len(bidsDepth))
	}
}

func TestDeltaRemoveAndAddLevels(t *testing.T) {

	ob := NewOrderBook(100)

	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 99.0, Quantity: 10.0}},
	)

	for i := 0; i < 100; i++ {

		ob.UpdateDelta(
			[]Order{{Price: 100.0 + float64(i), Quantity: 0}},
			[]Order{{Price: 99.0 - float64(i), Quantity: 0}},
		)

		ob.UpdateDelta(
			[]Order{{Price: 101.0 + float64(i), Quantity: 5.0}},
			[]Order{{Price: 98.0 - float64(i), Quantity: 5.0}},
		)

		_, _, askOk := ob.GetBestAsk()
		_, _, bidOk := ob.GetBestBid()
		if !askOk {
			t.Fatalf("asks became empty at iteration %d", i)
		}
		if !bidOk {
			t.Fatalf("bids became empty at iteration %d", i)
		}
	}
}

func TestDeltaRemoveNonExistent(t *testing.T) {

	ob := NewOrderBook(100)

	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 99.0, Quantity: 10.0}},
	)

	ob.UpdateDelta(
		[]Order{{Price: 999.0, Quantity: 0}},
		[]Order{{Price: 1.0, Quantity: 0}},
	)

	askPrice, _, ok := ob.GetBestAsk()
	if !ok || askPrice != 100.0 {
		t.Fatalf("expected ask at 100, got %v, ok=%v", askPrice, ok)
	}
	bidPrice, _, ok := ob.GetBestBid()
	if !ok || bidPrice != 99.0 {
		t.Fatalf("expected bid at 99, got %v, ok=%v", bidPrice, ok)
	}
}

func TestEmptyDelta(t *testing.T) {

	ob := NewOrderBook(100)

	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 99.0, Quantity: 10.0}},
	)

	ob.UpdateDelta([]Order{}, []Order{})

	askPrice, _, ok := ob.GetBestAsk()
	if !ok || askPrice != 100.0 {
		t.Fatalf("expected ask at 100, got %v, ok=%v", askPrice, ok)
	}
	bidPrice, _, ok := ob.GetBestBid()
	if !ok || bidPrice != 99.0 {
		t.Fatalf("expected bid at 99, got %v, ok=%v", bidPrice, ok)
	}
}

func TestRapidDeltaUpdates(t *testing.T) {

	ob := NewOrderBook(50)

	asks := make([]Order, 50)
	bids := make([]Order, 50)
	for i := 0; i < 50; i++ {
		asks[i] = Order{Price: 100.0 + float64(i)*0.1, Quantity: float64(i + 1)}
		bids[i] = Order{Price: 99.9 - float64(i)*0.1, Quantity: float64(i + 1)}
	}
	ob.UpdateSnapshot(asks, bids)

	for i := 0; i < 1000; i++ {

		deltaAsks := []Order{
			{Price: 100.0 + float64(i%50)*0.1, Quantity: float64((i % 10) + 1)},
		}
		deltaBids := []Order{
			{Price: 99.9 - float64(i%50)*0.1, Quantity: float64((i % 10) + 1)},
		}

		if i%7 == 0 {
			deltaAsks = append(deltaAsks, Order{Price: 200.0 + float64(i)*0.01, Quantity: 5})
			deltaBids = append(deltaBids, Order{Price: 50.0 - float64(i)*0.01, Quantity: 5})
		}

		if i%11 == 0 && i > 0 {
			deltaAsks = append(deltaAsks, Order{Price: 200.0 + float64(i-11)*0.01, Quantity: 0})
			deltaBids = append(deltaBids, Order{Price: 50.0 - float64(i-11)*0.01, Quantity: 0})
		}

		ob.UpdateDelta(deltaAsks, deltaBids)

		_, _, askOk := ob.GetBestAsk()
		_, _, bidOk := ob.GetBestBid()
		if !askOk {
			t.Fatalf("asks became empty at iteration %d", i)
		}
		if !bidOk {
			t.Fatalf("bids became empty at iteration %d", i)
		}
	}
}

func TestCrossLeafLinkage(t *testing.T) {

	ob := NewOrderBook(200)

	asks := make([]Order, 150)
	bids := make([]Order, 150)
	for i := 0; i < 150; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: float64(i + 1)}
		bids[i] = Order{Price: 1000.0 + float64(i), Quantity: float64(i + 1)}
	}
	ob.UpdateSnapshot(asks, bids)

	asksDepth, bidsDepth := ob.GetDepth(200)
	if len(asksDepth) != 150 {
		t.Fatalf("expected 150 asks from GetDepth, got %d (linked list may be broken)", len(asksDepth))
	}
	if len(bidsDepth) != 150 {
		t.Fatalf("expected 150 bids from GetDepth, got %d (linked list may be broken)", len(bidsDepth))
	}

	for i := 0; i < 149; i++ {
		if asksDepth[i].Price >= asksDepth[i+1].Price {
			t.Fatalf("asks not in ascending order at index %d: %v >= %v", i, asksDepth[i].Price, asksDepth[i+1].Price)
		}
		if bidsDepth[i].Price <= bidsDepth[i+1].Price {
			t.Fatalf("bids not in descending order at index %d: %v <= %v", i, bidsDepth[i].Price, bidsDepth[i+1].Price)
		}
	}

	ob.UpdateDelta(
		[]Order{
			{Price: 2031.5, Quantity: 100},
			{Price: 2063.5, Quantity: 100},
			{Price: 2095.5, Quantity: 100},
		},
		[]Order{
			{Price: 1031.5, Quantity: 100},
			{Price: 1063.5, Quantity: 100},
			{Price: 1095.5, Quantity: 100},
		},
	)

	asksDepth, bidsDepth = ob.GetDepth(200)
	if len(asksDepth) != 153 {
		t.Fatalf("expected 153 asks after adding boundary levels, got %d", len(asksDepth))
	}
	if len(bidsDepth) != 153 {
		t.Fatalf("expected 153 bids after adding boundary levels, got %d", len(bidsDepth))
	}

	for i := 0; i < len(asksDepth)-1; i++ {
		if asksDepth[i].Price >= asksDepth[i+1].Price {
			t.Fatalf("asks not in ascending order after boundary insert at index %d: %v >= %v", i, asksDepth[i].Price, asksDepth[i+1].Price)
		}
	}
	for i := 0; i < len(bidsDepth)-1; i++ {
		if bidsDepth[i].Price <= bidsDepth[i+1].Price {
			t.Fatalf("bids not in descending order after boundary insert at index %d: %v <= %v", i, bidsDepth[i].Price, bidsDepth[i+1].Price)
		}
	}
}

func TestStaleInternalNodePointer(t *testing.T) {

	ob := NewOrderBook(200)

	asks := make([]Order, 100)
	for i := 0; i < 100; i++ {
		asks[i] = Order{Price: 1000.0 + float64(i), Quantity: 1}
	}
	ob.UpdateSnapshot(asks, []Order{})

	asksDepth, _ := ob.GetDepth(200)
	if len(asksDepth) != 100 {
		t.Fatalf("expected 100 asks initially, got %d", len(asksDepth))
	}

	removeAsks := make([]Order, 96)
	for i := 0; i < 96; i++ {
		removeAsks[i] = Order{Price: 1000.0 + float64(i), Quantity: 0}
	}
	ob.UpdateDelta(removeAsks, []Order{})

	asksDepth, _ = ob.GetDepth(200)
	if len(asksDepth) != 4 {
		t.Fatalf("expected 4 asks after removal, got %d", len(asksDepth))
	}

	removeRest := []Order{
		{Price: 1096.0, Quantity: 0},
		{Price: 1097.0, Quantity: 0},
		{Price: 1098.0, Quantity: 0},
		{Price: 1099.0, Quantity: 0},
	}
	ob.UpdateDelta(removeRest, []Order{})

	asksDepth, _ = ob.GetDepth(200)
	if len(asksDepth) != 0 {
		t.Fatalf("expected 0 asks after removing all, got %d", len(asksDepth))
	}

	newAsks := make([]Order, 50)
	for i := 0; i < 50; i++ {
		newAsks[i] = Order{Price: 2000.0 + float64(i), Quantity: 1}
	}
	ob.UpdateDelta(newAsks, []Order{})

	asksDepth, _ = ob.GetDepth(200)
	if len(asksDepth) != 50 {
		t.Fatalf("expected 50 asks after adding new ones, got %d (tree structure corrupted after removals)", len(asksDepth))
	}

	askPrice, _, ok := ob.GetBestAsk()
	if !ok {
		t.Fatal("GetBestAsk returned ok=false, but we just added 50 asks!")
	}
	if askPrice != 2000.0 {
		t.Fatalf("expected best ask at 2000.0, got %v", askPrice)
	}
}

func TestFloatPrecisionIssue(t *testing.T) {

	ob := NewOrderBook(100)

	parseFloat := func(s string) float64 {
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}

	initialAsks := []Order{
		{Price: parseFloat("92617.7"), Quantity: 4.541},
		{Price: parseFloat("92618"), Quantity: 0.002},
		{Price: parseFloat("92618.4"), Quantity: 0.001},
		{Price: parseFloat("92618.5"), Quantity: 0.001},
	}
	ob.UpdateSnapshot(initialAsks, []Order{{Price: parseFloat("92617.6"), Quantity: 1}})

	asksDepth, _ := ob.GetDepth(10)
	if len(asksDepth) != 4 {
		t.Fatalf("expected 4 asks initially, got %d", len(asksDepth))
	}

	deltaAsks := []Order{
		{Price: parseFloat("92617.70"), Quantity: 0},
		{Price: parseFloat("92618.00"), Quantity: 0},
		{Price: parseFloat("92618.40"), Quantity: 0},
		{Price: parseFloat("92618.50"), Quantity: 0},
		{Price: parseFloat("92625.00"), Quantity: 0.018},
		{Price: parseFloat("92625.40"), Quantity: 0.003},
	}
	ob.UpdateDelta(deltaAsks, []Order{})

	asksDepth, _ = ob.GetDepth(10)
	t.Logf("After delta: %d asks", len(asksDepth))
	for _, a := range asksDepth {
		t.Logf("  price=%v qty=%v", a.Price, a.Quantity)
	}

	if len(asksDepth) != 2 {
		t.Fatalf("expected 2 asks after delta, got %d (float precision issue?)", len(asksDepth))
	}

	for _, a := range asksDepth {
		if a.Price < 92620 {
			t.Fatalf("old ask at price %v was not removed!", a.Price)
		}
	}
}

func TestInsertAtLeafBoundaries(t *testing.T) {
	ob := NewOrderBook(200)

	asks := make([]Order, 100)
	for i := 0; i < 100; i++ {
		asks[i] = Order{Price: 1000.0 + float64(i)*2, Quantity: 1}
	}
	ob.UpdateSnapshot(asks, []Order{})

	for i := 0; i < 100; i++ {
		ob.UpdateDelta(
			[]Order{{Price: 1001.0 + float64(i)*2, Quantity: 1}},
			[]Order{},
		)
	}

	asksDepth, _ := ob.GetDepth(300)
	if len(asksDepth) != 200 {
		t.Fatalf("expected 200 asks, got %d (linked list broken at leaf boundary)", len(asksDepth))
	}

	for i := 0; i < len(asksDepth)-1; i++ {
		if asksDepth[i].Price >= asksDepth[i+1].Price {
			t.Fatalf("asks not in order at index %d: %v >= %v", i, asksDepth[i].Price, asksDepth[i+1].Price)
		}

		if asksDepth[i+1].Price-asksDepth[i].Price != 1.0 {
			t.Fatalf("missing price at index %d: got %v and %v (diff=%v)", i, asksDepth[i].Price, asksDepth[i+1].Price, asksDepth[i+1].Price-asksDepth[i].Price)
		}
	}
}

func TestOrderbook1mCollision(t *testing.T) {
	ob := NewOrderBook(10)
	asks := make([]Order, 10)
	bids := make([]Order, 10)
	for i := 0; i < 10; i++ {
		asks[i] = Order{Price: 99.0 - float64(i), Quantity: 10.0}
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(bids, asks)
	printOb := func() {
		asks, bids := ob.GetDepth(10)
		printAsksBids(asks, bids)
	}
	printOb()

	asksDelta := []Order{
		{Price: 98.9, Quantity: 1},
	}
	bidsDelta := []Order{
		{Price: 98.8, Quantity: 1},
	}
	ob.UpdateSnapshot(asksDelta, bidsDelta)
	printOb()
	asksDelta = []Order{
		{Price: 97.9, Quantity: 1},
	}
	bidsDelta = []Order{
		{Price: 97.8, Quantity: 1},
	}
	ob.UpdateSnapshot(asksDelta, bidsDelta)
	printOb()
	ob.UpdateDelta(
		[]Order{{Price: 97.9, Quantity: 0}},
		[]Order{{Price: 97.8, Quantity: 0}})
	printOb()
}

func TestManualSplitCount(t *testing.T) {
	ob := NewOrderBook(1000)

	asks := make([]Order, 64)
	for i := 0; i < 64; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 1.0}
	}

	bids := []Order{{Price: 1999.0, Quantity: 1.0}}

	ob.UpdateSnapshot(asks, bids)

	count := ob.countLevels(ob.asks)
	t.Logf("Inserted 64 asks, counted %d in tree", count)

	if count != 64 {
		t.Errorf("SPLIT BUG: Expected 64 asks, got %d - lost %d elements during split!", count, 64-count)
	}

	asksDepth, _ := ob.GetDepth(100)
	if len(asksDepth) != 64 {
		t.Errorf("GetDepth: Expected 64 asks, got %d", len(asksDepth))
	}
}

func TestOrderBookMassUpdate(t *testing.T) {
	ob := NewOrderBook(100)
	asks := make([]Order, 100)
	bids := make([]Order, 100)
	for i := 0; i < 100; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 10.0}
		bids[i] = Order{Price: 1999.0 - float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(asks, bids)
	printOb := func() {
		asks, bids := ob.GetDepth(10)
		printAsksBids(asks, bids)
	}
	printOb()
	for k := range 50 {
		asks = make([]Order, k)
		bids = make([]Order, k)
		for i := 0; i < k; i++ {
			asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 0.0}
			bids[i] = Order{Price: 1999.0 - float64(i), Quantity: 0.0}
		}
		ob.UpdateDelta(asks, bids)

		asks = make([]Order, k)
		bids = make([]Order, k)
		for i := 0; i < k; i++ {
			asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 10.0}
			bids[i] = Order{Price: 1999.0 - float64(i), Quantity: 10.0}
		}
		ob.UpdateDelta(asks, bids)
	}
	printOb()
}

func TestSimpleRemoveAfterSplit(t *testing.T) {
	ob := NewOrderBook(100)

	asks := make([]Order, 70)
	for i := 0; i < 70; i++ {
		asks[i] = Order{Price: float64(1000 + i), Quantity: 1.0}
	}
	ob.UpdateSnapshot(asks, []Order{{Price: 999, Quantity: 1}})

	for i := 0; i < 70; i++ {
		priceFloat := float64(1000 + i)
		priceInt := toInt64(priceFloat)
		found := ob.findAsk(priceInt)
		if found == nil {
			t.Errorf("Price %.0f not found after insertion", priceFloat)
		}
	}

	ob.UpdateDelta([]Order{{Price: 1031, Quantity: 0}}, []Order{})

	found := ob.findAsk(toInt64(1031))
	if found != nil {
		t.Errorf("Price 1031 still found after removal! qty=%.4f", found.quantity)
	}

	asksDepth, _ := ob.GetDepth(100)
	if len(asksDepth) != 69 {
		t.Errorf("Expected 69 asks after removing 1, got %d", len(asksDepth))
		for i, ask := range asksDepth {
			if i < 5 || i >= len(asksDepth)-5 {
				t.Logf("  [%d] price=%.0f", i, ask.Price)
			}
		}
	}
}

func TestBidAsksOrder(t *testing.T) {
	ob := NewOrderBook(10)

	asks := make([]Order, 10)
	for i := 0; i < 10; i++ {
		asks[i] = Order{Price: float64(1010 - i), Quantity: 1.0}
	}
	bids := make([]Order, 10)
	for i := 0; i < 10; i++ {
		bids[i] = Order{Price: float64(990 + i), Quantity: 1.0}
	}
	ob.UpdateSnapshot(asks, bids)
	asks, bids = ob.GetDepth(2)
	printAsksBids(asks, bids)
}

// BenchmarkTrimToMaxDepth benchmarks the optimized trimToMaxDepth with large orderbooks
func BenchmarkTrimToMaxDepth(b *testing.B) {
	depths := []int{100, 500, 1000, 2000}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			ob := NewOrderBook(depth)

			// Create initial orderbook with depth+100 levels
			initialSize := depth + 100
			asks := make([]Order, initialSize)
			bids := make([]Order, initialSize)
			for i := 0; i < initialSize; i++ {
				asks[i] = Order{Price: 10000.0 + float64(i), Quantity: 1.0}
				bids[i] = Order{Price: 9999.0 - float64(i), Quantity: 1.0}
			}
			ob.UpdateSnapshot(asks, bids)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Add a new level that will trigger trimming
				ob.UpdateDelta(
					[]Order{{Price: 10000.0 + float64(initialSize+i), Quantity: 1.0}},
					[]Order{{Price: 9999.0 - float64(initialSize+i), Quantity: 1.0}},
				)
			}
		})
	}
}

// BenchmarkUpdateSnapshotWithDepthLimit benchmarks full snapshot updates with depth limiting
func BenchmarkUpdateSnapshotWithDepthLimit(b *testing.B) {
	depths := []int{100, 500, 1000, 2000}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			ob := NewOrderBook(depth)

			// Create snapshot with depth+500 levels (will be trimmed)
			snapshotSize := depth + 500
			asks := make([]Order, snapshotSize)
			bids := make([]Order, snapshotSize)
			for i := 0; i < snapshotSize; i++ {
				asks[i] = Order{Price: 10000.0 + float64(i), Quantity: 1.0 + rand.Float64()}
				bids[i] = Order{Price: 9999.0 - float64(i), Quantity: 1.0 + rand.Float64()}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Slightly modify quantities to simulate real updates
				for j := range asks {
					asks[j].Quantity = 1.0 + rand.Float64()
					bids[j].Quantity = 1.0 + rand.Float64()
				}
				ob.UpdateSnapshot(asks, bids)
			}
		})
	}
}
