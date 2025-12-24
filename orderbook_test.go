package orderbook

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func printAsksBids(asks, bids []Order) {
	fmt.Println("Asks:")

	for _, ask := range slices.Backward(asks) {
		fmt.Printf("Price: %f, Quantity: %f\n", ask.Price, ask.Quantity)
	}
	fmt.Println("--------------------")
	for _, bid := range bids {
		fmt.Printf("Price: %f, Quantity: %f\n", bid.Price, bid.Quantity)
	}
	fmt.Println("BIDS")
}

func TestRemove(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 101.0, Quantity: 10.0}},
	)

	price, qty, ok := ob.GetBestBid()
	if !ok || price != 100.0 || qty != 10.0 {
		t.Fatalf("expected price=100, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
	price, qty, ok = ob.GetBestAsk()
	if !ok || price != 101.0 || qty != 10.0 {
		t.Fatalf("expected price=101, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
	ob.removeBid(100.0)
	ob.removeAsk(101.0)

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
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{},
	)

	price, qty, ok := ob.GetBestBid()
	if !ok || price != 100.0 || qty != 10.0 {
		t.Fatalf("expected price=100, qty=10, got price=%v, qty=%v, ok=%v", price, qty, ok)
	}
}

func TestInsertAndRetrieveAsk(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{},
		[]Order{{Price: 100.0, Quantity: 10.0}},
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
	ob.UpdateSnapshot(bids, []Order{})

	price, qty, ok := ob.GetBestBid()
	if !ok || price != 101.0 || qty != 20.0 {
		t.Fatalf("expected best bid price=101, qty=20, got price=%v, qty=%v", price, qty)
	}

	bidsDepth, _ := ob.GetDepth(10)
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
	ob.UpdateSnapshot([]Order{}, asks)

	price, qty, ok := ob.GetBestAsk()
	if !ok || price != 99.0 || qty != 20.0 {
		t.Fatalf("expected best ask price=99, qty=20, got price=%v, qty=%v", price, qty)
	}

	_, asksDepth := ob.GetDepth(10)
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
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{},
	)

	ob.UpdateDelta(
		[]Order{{Price: 101.0, Quantity: 20.0}},
		[]Order{},
	)

	bidsDepth, _ := ob.GetDepth(10)
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
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{},
	)

	ob.UpdateDelta(
		[]Order{{Price: 100.0, Quantity: 15.0}},
		[]Order{},
	)

	_, qty, ok := ob.GetBestBid()
	if !ok || qty != 15.0 {
		t.Fatalf("expected qty=15, got qty=%v", qty)
	}
}

func TestDeltaUpdateRemoveLevel(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}, {Price: 101.0, Quantity: 20.0}},
		[]Order{},
	)

	ob.UpdateDelta(
		[]Order{{Price: 100.0, Quantity: 0}},
		[]Order{},
	)

	bidsDepth, _ := ob.GetDepth(10)
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
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 102.0, Quantity: 10.0}},
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
	ob.UpdateSnapshot(bids, []Order{})

	bidsDepth, _ := ob.GetDepth(10)
	if len(bidsDepth) != 10 {
		t.Fatalf("expected 10 bids, got %d", len(bidsDepth))
	}
}

func TestConcurrentReadsWithSnapshot(t *testing.T) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 102.0, Quantity: 10.0}},
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
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 102.0, Quantity: 10.0}},
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
	ob := NewOrderBook(100)
	numLevels := 1000

	bids := make([]Order, numLevels)
	asks := make([]Order, numLevels)

	for i := 0; i < numLevels; i++ {
		bids[i] = Order{Price: 1000.0 + float64(i)*0.01, Quantity: float64(i + 1)}
		asks[i] = Order{Price: 2000.0 + float64(i)*0.01, Quantity: float64(i + 1)}
	}

	ob.UpdateSnapshot(bids, asks)

	bidsDepth, asksDepth := ob.GetDepth(numLevels)
	if len(bidsDepth) != numLevels || len(asksDepth) != numLevels {
		t.Fatalf("expected full depth, got %d bids, %d asks", len(bidsDepth), len(asksDepth))
	}
}

func TestOrderBookSmallSnapshotUpdate(t *testing.T) {
	ob := NewOrderBook(100)
	numLevels := 1000

	// Ask - Seller's price - the minimum price a seller is willing to accept
	// Bid - Buyer's price - the maximum price a buyer is willing to pay
	asks := make([]Order, numLevels)
	bids := make([]Order, numLevels)

	for i := 0; i < numLevels; i++ {
		asks[i] = Order{Price: 2000.0 + float64(i), Quantity: 1}
		bids[i] = Order{Price: 1000.0 + float64(i), Quantity: 1}
	}
	ob.UpdateSnapshot(bids, asks)
	bidsDepth, asksDepth := ob.GetDepth(numLevels)
	if len(bidsDepth) != numLevels || len(asksDepth) != numLevels {
		t.Fatalf("expected full depth, got %d bids, %d asks", len(bidsDepth), len(asksDepth))
	}
	bids, asks = ob.GetDepth(1)
	fmt.Printf("Best Bid: Price=%f, Quantity=%f\n", bids[0].Price, bids[0].Quantity)
	fmt.Printf("Best Ask: Price=%f, Quantity=%f\n", asks[0].Price, asks[0].Quantity)

	bids = []Order{{Price: 1995.0, Quantity: 1}}
	asks = []Order{{Price: 1996.0, Quantity: 1}}
	ob.UpdateSnapshot(bids, asks)
	// After reciving a small snapshot, the order book should update correctly
	// highest bid should be 1995 and lowest ask should be 1996

	bids, asks = ob.GetDepth(1)
	fmt.Printf("Best Bid: Price=%f, Quantity=%f\n", bids[0].Price, bids[0].Quantity)
	fmt.Printf("Best Ask: Price=%f, Quantity=%f\n", asks[0].Price, asks[0].Quantity)
}

func BenchmarkGetBestBid(b *testing.B) {
	ob := NewOrderBook(100)
	bids := make([]Order, 100)
	for i := 0; i < 100; i++ {
		bids[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(bids, []Order{})

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
		asks[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(bids, asks)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.GetDepth(10)
	}
}

func BenchmarkUpdateDelta(b *testing.B) {
	ob := NewOrderBook(100)
	ob.UpdateSnapshot(
		[]Order{{Price: 100.0, Quantity: 10.0}},
		[]Order{{Price: 102.0, Quantity: 10.0}},
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
		asks[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(bids, asks)

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
		asks[i] = Order{Price: 100.0 + float64(i), Quantity: 10.0}
	}
	ob.UpdateSnapshot(bids, asks)

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
