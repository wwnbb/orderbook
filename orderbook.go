package orderbook

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
)

// Степень B-дерева (минимальное количество ключей в узле = btreeDegree-1)
const btreeDegree = 32

// priceScale - множитель для конвертации float64 в int64 (8 знаков после запятой)
const priceScale = 100000000.0 // 1e8

// toInt64 конвертирует float64 цену в int64 для внутреннего хранения
func toInt64(f float64) int64 {
	return int64(math.Round(f * priceScale))
}

// toFloat64 конвертирует int64 цену обратно в float64 для публичного API
func toFloat64(i int64) float64 {
	return float64(i) / priceScale
}

type Order struct {
	Price     float64
	Quantity  float64
	Timestamp int64
}

type priceLevel struct {
	price    int64 // Internal representation in int64
	quantity float64
	next     *priceLevel
	prev     *priceLevel
}

type bnode struct {
	prices   []int64 // Internal representation in int64
	levels   []*priceLevel
	children []*bnode
	next     *bnode
	prev     *bnode
	parent   *bnode
	isLeaf   bool
}

type OrderBook struct {
	maxDepth int

	mu       sync.RWMutex
	bids     *bnode
	asks     *bnode
	bidHead  *priceLevel // highest bid (best bid)
	askHead  *priceLevel // lowest ask (best ask)
	bidTail  *priceLevel // lowest bid (worst bid)
	askTail  *priceLevel // highest ask (worst ask)
	bidDepth int         // current number of bid levels
	askDepth int         // current number of ask levels
	version  uint64
}

func NewOrderBook(maxDepth int) *OrderBook {
	return &OrderBook{
		maxDepth: maxDepth,
		bids:     &bnode{isLeaf: true},
		asks:     &bnode{isLeaf: true},
	}
}

func (ob *OrderBook) rebuildHeads() {
	ob.bidHead = ob.findHighestBid(ob.bids)
	ob.askHead = ob.findLowestAsk(ob.asks)
	ob.bidTail = ob.findLowestBid(ob.bids)
	ob.askTail = ob.findHighestAsk(ob.asks)
	ob.bidDepth = ob.countLevelsFromHead(ob.bidHead, true)
	ob.askDepth = ob.countLevelsFromHead(ob.askHead, false)
}

func (ob *OrderBook) findHighestBid(n *bnode) *priceLevel {
	if n == nil {
		return nil
	}
	for !n.isLeaf {
		if len(n.children) == 0 {
			return nil
		}
		n = n.children[len(n.children)-1]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[len(n.levels)-1]
}

func (ob *OrderBook) findLowestAsk(n *bnode) *priceLevel {
	if n == nil {
		return nil
	}
	for !n.isLeaf {
		if len(n.children) == 0 {
			return nil
		}
		n = n.children[0]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[0]
}

func (ob *OrderBook) findLowestBid(n *bnode) *priceLevel {
	if n == nil {
		return nil
	}
	for !n.isLeaf {
		if len(n.children) == 0 {
			return nil
		}
		n = n.children[0]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[0]
}

func (ob *OrderBook) findHighestAsk(n *bnode) *priceLevel {
	if n == nil {
		return nil
	}
	for !n.isLeaf {
		if len(n.children) == 0 {
			return nil
		}
		n = n.children[len(n.children)-1]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[len(n.levels)-1]
}

// countLevelsFromHead counts levels starting from head using linked list
// isBid: true for bids (walk prev), false for asks (walk next)
func (ob *OrderBook) countLevelsFromHead(head *priceLevel, isBid bool) int {
	if head == nil {
		return 0
	}
	count := 1
	curr := head
	if isBid {
		for curr.prev != nil {
			count++
			curr = curr.prev
		}
	} else {
		for curr.next != nil {
			count++
			curr = curr.next
		}
	}
	return count
}

// countLevels подсчитывает количество уровней в дереве
func (ob *OrderBook) countLevels(n *bnode) int {
	if n == nil {
		return 0
	}
	count := 0
	for !n.isLeaf {
		if len(n.children) == 0 {
			return 0
		}
		n = n.children[0]
	}
	// Проходим по всем листьям через связный список
	for n != nil {
		count += len(n.levels)
		n = n.next
	}
	return count
}

func (ob *OrderBook) trimToMaxDepth() {
	if ob.maxDepth <= 0 {
		return
	}

	// Optimized: O(1) check + O(log N) removal instead of O(N) traversal
	// Remove worst bids (lowest prices) if depth exceeds limit
	for ob.bidDepth > ob.maxDepth && ob.bidTail != nil {
		toRemove := ob.bidTail.price
		ob.removeFromTree(ob.bids, toRemove, true)
		ob.bidHead = ob.findHighestBid(ob.bids)
		ob.bidTail = ob.findLowestBid(ob.bids)
		ob.bidDepth--
		if ob.bidDepth < 0 {
			ob.bidDepth = 0
		}
	}

	// Remove worst asks (highest prices) if depth exceeds limit
	for ob.askDepth > ob.maxDepth && ob.askTail != nil {
		toRemove := ob.askTail.price
		ob.removeFromTree(ob.asks, toRemove, false)
		ob.askHead = ob.findLowestAsk(ob.asks)
		ob.askTail = ob.findHighestAsk(ob.asks)
		ob.askDepth--
		if ob.askDepth < 0 {
			ob.askDepth = 0
		}
	}
}

func (ob *OrderBook) UpdateSnapshot(asks, bids []Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// Bids processing
	if len(bids) > 0 {
		highestBidInt := toInt64(bids[0].Price)
		for _, bid := range bids {
			bidInt := toInt64(bid.Price)
			if bidInt > highestBidInt {
				highestBidInt = bidInt
			}
		}

		toRemove := make([]int64, 0)
		curr := ob.bidHead
		for curr != nil && curr.price > highestBidInt {
			toRemove = append(toRemove, curr.price)
			curr = curr.prev
		}
		for _, p := range toRemove {
			ob.removeFromTree(ob.bids, p, true)
		}

		for _, order := range bids {
			if order.Quantity > 0 {
				priceInt := toInt64(order.Price)
				level := ob.findBid(priceInt)
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
		lowestAskInt := toInt64(asks[0].Price)
		for _, ask := range asks {
			askInt := toInt64(ask.Price)
			if askInt < lowestAskInt {
				lowestAskInt = askInt
			}
		}

		toRemove := make([]int64, 0)
		curr := ob.askHead
		for curr != nil && curr.price < lowestAskInt {
			toRemove = append(toRemove, curr.price)
			curr = curr.next
		}
		for _, p := range toRemove {
			ob.removeFromTree(ob.asks, p, false)
		}

		for _, order := range asks {
			if order.Quantity > 0 {
				priceInt := toInt64(order.Price)
				level := ob.findAsk(priceInt)
				if level != nil {
					level.quantity = order.Quantity
				} else {
					ob.insertAsk(order.Price, order.Quantity)
				}
			}
		}
	}

	ob.rebuildHeads()

	// Исправление пересечений bid >= ask (symmetric fix for both sides)
	// Iterate until collision is resolved or one side is empty
	for ob.bidHead != nil && ob.askHead != nil && ob.bidHead.price >= ob.askHead.price {
		// Remove bids >= lowest ask
		toRemove := make([]int64, 0)
		curr := ob.bidHead
		for curr != nil && curr.price >= ob.askHead.price {
			toRemove = append(toRemove, curr.price)
			curr = curr.prev
		}
		for _, p := range toRemove {
			ob.removeFromTree(ob.bids, p, true)
		}
		ob.bidHead = ob.findHighestBid(ob.bids)

		// Also remove asks <= highest bid (symmetric fix)
		if ob.bidHead != nil && ob.askHead != nil && ob.bidHead.price >= ob.askHead.price {
			toRemove = make([]int64, 0)
			curr = ob.askHead
			for curr != nil && curr.price <= ob.bidHead.price {
				toRemove = append(toRemove, curr.price)
				curr = curr.next
			}
			for _, p := range toRemove {
				ob.removeFromTree(ob.asks, p, false)
			}
			ob.askHead = ob.findLowestAsk(ob.asks)
		}
	}

	ob.trimToMaxDepth()
	// ob.validateAndPanic(asks, bids)
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
	ob.trimToMaxDepth()
	// ob.validateAndPanic(asks, bids)
	atomic.AddUint64(&ob.version, 1)
}

func (ob *OrderBook) GetBestBid() (price float64, quantity float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.bidHead == nil {
		return 0, 0, false
	}
	return toFloat64(ob.bidHead.price), ob.bidHead.quantity, true
}

func (ob *OrderBook) GetBestAsk() (price float64, quantity float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.askHead == nil {
		return 0, 0, false
	}
	return toFloat64(ob.askHead.price), ob.askHead.quantity, true
}

func (ob *OrderBook) GetDepth(levels int) (asks, bids []Order) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return ob.getDepthInternal(levels)
}

// getDepthInternal is the internal version of GetDepth without locking
// Used when we already hold the lock (e.g., during validation)
func (ob *OrderBook) getDepthInternal(levels int) (asks, bids []Order) {
	bids = make([]Order, 0, levels)
	asks = make([]Order, 0, levels)

	curr := ob.bidHead
	for curr != nil && len(bids) < levels {
		bids = append(bids, Order{Price: toFloat64(curr.price), Quantity: curr.quantity})
		curr = curr.prev
	}

	curr = ob.askHead
	for curr != nil && len(asks) < levels {
		asks = append(asks, Order{Price: toFloat64(curr.price), Quantity: curr.quantity})
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
	return toFloat64(ob.bidHead.price+ob.askHead.price) / 2, true
}

func (ob *OrderBook) GetSpread() (spread float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.bidHead == nil || ob.askHead == nil {
		return 0, false
	}
	return toFloat64(ob.askHead.price - ob.bidHead.price), true
}

func (ob *OrderBook) GetVersion() uint64 {
	return atomic.LoadUint64(&ob.version)
}

func (ob *OrderBook) insertBid(price float64, quantity float64) {
	priceInt := toInt64(price)
	level := ob.searchOrInsert(ob.bids, priceInt, true)
	if level != nil {
		level.quantity = quantity
	}
}

func (ob *OrderBook) insertAsk(price float64, quantity float64) {
	priceInt := toInt64(price)
	level := ob.searchOrInsert(ob.asks, priceInt, false)
	if level != nil {
		level.quantity = quantity
	}
}

func (ob *OrderBook) updateBid(price float64, quantity float64) {
	if quantity == 0 {
		ob.removeBid(price)
	} else {
		priceInt := toInt64(price)
		level := ob.findBid(priceInt)
		if level != nil {
			level.quantity = quantity
		} else {
			ob.insertBid(price, quantity)
		}
	}
}

func (ob *OrderBook) updateAsk(price float64, quantity float64) {
	if quantity == 0 {
		ob.removeAsk(price)
	} else {
		priceInt := toInt64(price)
		level := ob.findAsk(priceInt)
		if level != nil {
			level.quantity = quantity
		} else {
			ob.insertAsk(price, quantity)
		}
	}
}

func (ob *OrderBook) removeBid(price float64) {
	priceInt := toInt64(price)
	// Check if the price exists before removing
	if ob.findBid(priceInt) == nil {
		return
	}
	ob.removeFromTree(ob.bids, priceInt, true)
	ob.bidHead = ob.findHighestBid(ob.bids)
	ob.bidTail = ob.findLowestBid(ob.bids)
	ob.bidDepth--
	if ob.bidDepth < 0 {
		ob.bidDepth = 0
	}
}

func (ob *OrderBook) removeAsk(price float64) {
	priceInt := toInt64(price)
	// Check if the price exists before removing
	if ob.findAsk(priceInt) == nil {
		return
	}
	ob.removeFromTree(ob.asks, priceInt, false)
	ob.askHead = ob.findLowestAsk(ob.asks)
	ob.askTail = ob.findHighestAsk(ob.asks)
	ob.askDepth--
	if ob.askDepth < 0 {
		ob.askDepth = 0
	}
}

func (ob *OrderBook) findBid(priceInt int64) *priceLevel {
	return ob.search(ob.bids, priceInt)
}

func (ob *OrderBook) findAsk(priceInt int64) *priceLevel {
	return ob.search(ob.asks, priceInt)
}

// search ищет priceLevel по цене в B-дереве
func (ob *OrderBook) search(n *bnode, priceInt int64) *priceLevel {
	if n == nil {
		return nil
	}

	for {
		idx := ob.findKeyIndex(n.prices, priceInt)

		if idx < len(n.prices) && n.prices[idx] == priceInt {
			if n.isLeaf {
				return n.levels[idx]
			}
			// Во внутренних узлах ищем в правом поддереве
			if idx+1 < len(n.children) {
				n = n.children[idx+1]
				continue
			}
			return nil
		}

		if n.isLeaf {
			return nil
		}

		if idx < len(n.children) {
			n = n.children[idx]
		} else {
			return nil
		}
	}
}

// findKeyIndex находит индекс первого ключа >= price
func (ob *OrderBook) findKeyIndex(prices []int64, priceInt int64) int {
	lo, hi := 0, len(prices)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if prices[mid] < priceInt {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// searchOrInsert ищет или вставляет priceLevel
func (ob *OrderBook) searchOrInsert(root *bnode, priceInt int64, isBid bool) *priceLevel {
	if root == nil {
		return nil
	}

	// Если корень переполнен, разделяем его заранее
	if len(root.prices) >= 2*btreeDegree-1 {
		newRoot := &bnode{isLeaf: false}
		newRoot.children = []*bnode{root}
		root.parent = newRoot
		ob.splitChild(newRoot, 0)
		if isBid {
			ob.bids = newRoot
		} else {
			ob.asks = newRoot
		}
		root = newRoot
	}

	return ob.insertNonFull(root, priceInt, isBid)
}

// insertNonFull вставляет в узел, который гарантированно не полон
func (ob *OrderBook) insertNonFull(n *bnode, priceInt int64, isBid bool) *priceLevel {
	for {
		idx := ob.findKeyIndex(n.prices, priceInt)

		// Если ключ уже существует
		if idx < len(n.prices) && n.prices[idx] == priceInt {
			if n.isLeaf {
				return n.levels[idx]
			}
			// Для внутреннего узла, спускаемся в правое поддерево
			if idx+1 < len(n.children) {
				n = n.children[idx+1]
				continue
			}
			return nil
		}

		if n.isLeaf {
			// Вставляем новый уровень
			level := &priceLevel{price: priceInt}
			ob.insertIntoLeafAt(n, idx, priceInt, level)
			return level
		}

		// Спускаемся в дочерний узел
		if idx >= len(n.children) {
			return nil
		}

		child := n.children[idx]

		// Если дочерний узел полон, разделяем его
		if len(child.prices) >= 2*btreeDegree-1 {
			ob.splitChild(n, idx)
			// После разделения определяем, в какой узел идти
			if priceInt > n.prices[idx] {
				idx++
			} else if priceInt == n.prices[idx] {
				// Ключ поднялся из разделения
				if n.isLeaf {
					return n.levels[idx]
				}
			}
			if idx >= len(n.children) {
				return nil
			}
			child = n.children[idx]
		}

		n = child
	}
}

// insertIntoLeafAt вставляет уровень в лист на позицию idx
func (ob *OrderBook) insertIntoLeafAt(n *bnode, idx int, priceInt int64, level *priceLevel) {
	// Расширяем слайсы
	n.prices = append(n.prices, 0)
	copy(n.prices[idx+1:], n.prices[idx:])
	n.prices[idx] = priceInt

	n.levels = append(n.levels, nil)
	copy(n.levels[idx+1:], n.levels[idx:])
	n.levels[idx] = level

	// Связываем с соседями внутри узла
	if idx > 0 {
		level.prev = n.levels[idx-1]
		n.levels[idx-1].next = level
	}
	if idx < len(n.levels)-1 {
		level.next = n.levels[idx+1]
		n.levels[idx+1].prev = level
	}

	// Связываем с соседними узлами
	if idx == 0 && n.prev != nil && len(n.prev.levels) > 0 {
		prevLevel := n.prev.levels[len(n.prev.levels)-1]
		level.prev = prevLevel
		prevLevel.next = level
	}
	if idx == len(n.levels)-1 && n.next != nil && len(n.next.levels) > 0 {
		nextLevel := n.next.levels[0]
		level.next = nextLevel
		nextLevel.prev = level
	}
}

// splitChild разделяет полный дочерний узел
func (ob *OrderBook) splitChild(parent *bnode, childIdx int) {
	fullNode := parent.children[childIdx]
	mid := btreeDegree - 1

	// Создаём новый узел с правой половиной
	// Для B+ дерева: в листьях сохраняем все данные, включая separator
	// Для внутренних узлов: separator переходит в родителя
	var newPricesLen int
	var copyStartIdx int

	if fullNode.isLeaf {
		// B+ tree: keep separator in right child (data stays in leaves)
		newPricesLen = len(fullNode.prices) - mid
		copyStartIdx = mid
	} else {
		// Regular B-tree: separator goes to parent only
		newPricesLen = len(fullNode.prices) - mid - 1
		copyStartIdx = mid + 1
	}

	newNode := &bnode{
		isLeaf: fullNode.isLeaf,
		parent: parent,
		prices: make([]int64, newPricesLen),
		prev:   fullNode,
	}

	copy(newNode.prices, fullNode.prices[copyStartIdx:])

	if fullNode.isLeaf {
		newNode.levels = make([]*priceLevel, newPricesLen)
		copy(newNode.levels, fullNode.levels[copyStartIdx:])

		// Обновляем связи между листьями
		newNode.next = fullNode.next
		if fullNode.next != nil {
			fullNode.next.prev = newNode
		}
		fullNode.next = newNode
	} else {
		newNode.children = make([]*bnode, len(fullNode.children)-mid-1)
		copy(newNode.children, fullNode.children[mid+1:])
		// Обновляем родителя для перемещённых детей
		for _, child := range newNode.children {
			if child != nil {
				child.parent = newNode
			}
		}
	}

	// Ключ, который поднимается в родителя
	midKey := fullNode.prices[mid]

	// Обрезаем полный узел
	fullNode.prices = fullNode.prices[:mid]
	if fullNode.isLeaf {
		fullNode.levels = fullNode.levels[:mid]
	} else {
		fullNode.children = fullNode.children[:mid+1]
	}

	// Вставляем средний ключ в родителя
	parent.prices = append(parent.prices, 0)
	copy(parent.prices[childIdx+1:], parent.prices[childIdx:])
	parent.prices[childIdx] = midKey

	// Вставляем новый узел в родителя
	parent.children = append(parent.children, nil)
	copy(parent.children[childIdx+2:], parent.children[childIdx+1:])
	parent.children[childIdx+1] = newNode
}

// removeFromTree удаляет элемент из B-дерева
func (ob *OrderBook) removeFromTree(root *bnode, priceInt int64, isBid bool) {
	if root == nil || (len(root.prices) == 0 && len(root.children) == 0) {
		return
	}

	ob.removeRecursive(root, priceInt, isBid)

	// Если корень пуст, но есть дети, делаем ребёнка новым корнем
	if len(root.prices) == 0 && len(root.children) > 0 {
		newRoot := root.children[0]
		newRoot.parent = nil
		if isBid {
			ob.bids = newRoot
		} else {
			ob.asks = newRoot
		}
	}
}

func (ob *OrderBook) removeRecursive(n *bnode, priceInt int64, isBid bool) bool {
	idx := ob.findKeyIndex(n.prices, priceInt)

	if n.isLeaf {
		if idx < len(n.prices) && n.prices[idx] == priceInt {
			ob.removeFromLeafAt(n, idx)
			return true
		}
		return false
	}

	// Внутренний узел
	// В B+ дереве, если ключ совпадает с separator в внутреннем узле,
	// продолжаем поиск в правом поддереве, так как данные находятся только в листьях
	if idx < len(n.prices) && n.prices[idx] == priceInt {
		// Переходим в правое поддерево где находятся ключи >= separator
		idx = idx + 1
	}

	// Ключ в поддереве
	if idx >= len(n.children) {
		return false
	}

	child := n.children[idx]

	// Обеспечиваем, что у ребёнка достаточно ключей
	if len(child.prices) < btreeDegree {
		ob.ensureMinKeys(n, idx)
		// После балансировки нужно пересчитать индекс и проверить, не переместился ли ключ
		idx = ob.findKeyIndex(n.prices, priceInt)

		// Ключ мог переместиться в текущий узел после балансировки
		if idx < len(n.prices) && n.prices[idx] == priceInt {
			return ob.removeFromInternal(n, idx, isBid)
		}

		if idx >= len(n.children) {
			idx = len(n.children) - 1
		}
		if idx < 0 {
			return false
		}
		child = n.children[idx]
	}

	return ob.removeRecursive(child, priceInt, isBid)
}

// removeFromLeafAt удаляет элемент из листа по индексу
func (ob *OrderBook) removeFromLeafAt(n *bnode, idx int) {
	level := n.levels[idx]

	// Отвязываем от двусвязного списка
	if level.prev != nil {
		level.prev.next = level.next
	}
	if level.next != nil {
		level.next.prev = level.prev
	}

	// Удаляем из слайсов
	n.prices = append(n.prices[:idx], n.prices[idx+1:]...)
	n.levels = append(n.levels[:idx], n.levels[idx+1:]...)
}

// removeFromInternal удаляет ключ из внутреннего узла
func (ob *OrderBook) removeFromInternal(n *bnode, idx int, isBid bool) bool {
	leftChild := n.children[idx]
	rightChild := n.children[idx+1]

	if len(leftChild.prices) >= btreeDegree {
		// Находим предшественника
		pred := ob.findMax(leftChild)
		n.prices[idx] = pred.price
		return ob.removeRecursive(leftChild, pred.price, isBid)
	} else if len(rightChild.prices) >= btreeDegree {
		// Находим преемника
		succ := ob.findMin(rightChild)
		n.prices[idx] = succ.price
		return ob.removeRecursive(rightChild, succ.price, isBid)
	} else {
		// Объединяем детей
		price := n.prices[idx]
		ob.mergeChildren(n, idx)
		return ob.removeRecursive(leftChild, price, isBid)
	}
}

// findMin находит минимальный элемент в поддереве
func (ob *OrderBook) findMin(n *bnode) *priceLevel {
	for !n.isLeaf {
		n = n.children[0]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[0]
}

// findMax находит максимальный элемент в поддереве
func (ob *OrderBook) findMax(n *bnode) *priceLevel {
	for !n.isLeaf {
		n = n.children[len(n.children)-1]
	}
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[len(n.levels)-1]
}

// ensureMinKeys гарантирует, что у дочернего узла достаточно ключей
func (ob *OrderBook) ensureMinKeys(parent *bnode, childIdx int) {
	// Пробуем занять у левого соседа
	if childIdx > 0 {
		leftSibling := parent.children[childIdx-1]
		if len(leftSibling.prices) >= btreeDegree {
			ob.borrowFromLeft(parent, childIdx)
			return
		}
	}

	// Пробуем занять у правого соседа
	if childIdx < len(parent.children)-1 {
		rightSibling := parent.children[childIdx+1]
		if len(rightSibling.prices) >= btreeDegree {
			ob.borrowFromRight(parent, childIdx)
			return
		}
	}

	// Объединяем с соседом
	if childIdx > 0 {
		ob.mergeChildren(parent, childIdx-1)
	} else if childIdx < len(parent.children)-1 {
		ob.mergeChildren(parent, childIdx)
	}
}

func (ob *OrderBook) borrowFromLeft(parent *bnode, childIdx int) {
	child := parent.children[childIdx]
	leftSibling := parent.children[childIdx-1]

	if child.isLeaf {
		lastLevel := leftSibling.levels[len(leftSibling.levels)-1]
		lastPrice := leftSibling.prices[len(leftSibling.prices)-1]

		child.prices = append([]int64{lastPrice}, child.prices...)
		child.levels = append([]*priceLevel{lastLevel}, child.levels...)

		leftSibling.prices = leftSibling.prices[:len(leftSibling.prices)-1]
		leftSibling.levels = leftSibling.levels[:len(leftSibling.levels)-1]

		parent.prices[childIdx-1] = child.prices[0]
	} else {
		child.prices = append([]int64{parent.prices[childIdx-1]}, child.prices...)

		lastChild := leftSibling.children[len(leftSibling.children)-1]
		child.children = append([]*bnode{lastChild}, child.children...)
		if lastChild != nil {
			lastChild.parent = child
		}

		parent.prices[childIdx-1] = leftSibling.prices[len(leftSibling.prices)-1]

		leftSibling.prices = leftSibling.prices[:len(leftSibling.prices)-1]
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
	}
}

func (ob *OrderBook) borrowFromRight(parent *bnode, childIdx int) {
	child := parent.children[childIdx]
	rightSibling := parent.children[childIdx+1]

	if child.isLeaf {
		firstLevel := rightSibling.levels[0]
		firstPrice := rightSibling.prices[0]

		child.prices = append(child.prices, firstPrice)
		child.levels = append(child.levels, firstLevel)

		rightSibling.prices = rightSibling.prices[1:]
		rightSibling.levels = rightSibling.levels[1:]

		parent.prices[childIdx] = rightSibling.prices[0]
	} else {
		child.prices = append(child.prices, parent.prices[childIdx])

		firstChild := rightSibling.children[0]
		child.children = append(child.children, firstChild)
		if firstChild != nil {
			firstChild.parent = child
		}

		parent.prices[childIdx] = rightSibling.prices[0]

		rightSibling.prices = rightSibling.prices[1:]
		rightSibling.children = rightSibling.children[1:]
	}
}

func (ob *OrderBook) mergeChildren(parent *bnode, idx int) {
	leftChild := parent.children[idx]
	rightChild := parent.children[idx+1]

	if leftChild.isLeaf {
		leftChild.prices = append(leftChild.prices, rightChild.prices...)
		leftChild.levels = append(leftChild.levels, rightChild.levels...)
		leftChild.next = rightChild.next
		if rightChild.next != nil {
			rightChild.next.prev = leftChild
		}
	} else {
		leftChild.prices = append(leftChild.prices, parent.prices[idx])
		leftChild.prices = append(leftChild.prices, rightChild.prices...)
		leftChild.children = append(leftChild.children, rightChild.children...)
		for _, child := range rightChild.children {
			if child != nil {
				child.parent = leftChild
			}
		}
	}

	parent.prices = append(parent.prices[:idx], parent.prices[idx+1:]...)
	parent.children = append(parent.children[:idx+1], parent.children[idx+2:]...)
}

func (ob *OrderBook) Clear() {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	ob.bids = &bnode{isLeaf: true}
	ob.asks = &bnode{isLeaf: true}
	ob.bidHead = nil
	ob.askHead = nil
	ob.bidTail = nil
	ob.askTail = nil
	ob.bidDepth = 0
	ob.askDepth = 0
	atomic.AddUint64(&ob.version, 1)
}

func (ob *OrderBook) IsEmpty() bool {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	return ob.bidHead == nil && ob.askHead == nil
}

func hasNonZeroQuantity(orders []Order) bool {
	for _, order := range orders {
		if order.Quantity > 0 {
			return true
		}
	}
	return false
}

func (ob *OrderBook) dumpTreeStructure(side string, root *bnode) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n=== %s Tree Structure ===\n", side))
	if root == nil {
		sb.WriteString("  (nil)\n")
		return sb.String()
	}
	ob.dumpNode(&sb, root, 0)
	return sb.String()
}

func (ob *OrderBook) dumpNode(sb *strings.Builder, n *bnode, depth int) {
	if n == nil {
		return
	}

	indent := strings.Repeat("  ", depth)
	nodeType := "Internal"
	if n.isLeaf {
		nodeType = "Leaf"
	}

	sb.WriteString(fmt.Sprintf("%s[%s] prices=%d, children=%d\n", indent, nodeType, len(n.prices), len(n.children)))
	sb.WriteString(fmt.Sprintf("%s  prices: %v\n", indent, n.prices))

	if n.isLeaf && len(n.levels) > 0 {
		sb.WriteString(fmt.Sprintf("%s  levels: [", indent))
		for i, level := range n.levels {
			if i > 0 {
				sb.WriteString(", ")
			}
			if level != nil {
				sb.WriteString(fmt.Sprintf("%.2f:%.4f", toFloat64(level.price), level.quantity))
			} else {
				sb.WriteString("nil")
			}
		}
		sb.WriteString("]\n")
	}

	if !n.isLeaf {
		for i, child := range n.children {
			sb.WriteString(fmt.Sprintf("%s  child[%d]:\n", indent, i))
			ob.dumpNode(sb, child, depth+2)
		}
	}
}

func (ob *OrderBook) validateAndPanic(asksInput, bidsInput []Order) {
	hasNonZeroAsks := hasNonZeroQuantity(asksInput)
	hasNonZeroBids := hasNonZeroQuantity(bidsInput)

	asks, bids := ob.getDepthInternal(1)
	emptyAsks := len(asks) == 0
	emptyBids := len(bids) == 0

	hadCollision := false
	if hasNonZeroAsks && hasNonZeroBids {
		highestBidInt := int64(-1)
		lowestAskInt := int64(-1)

		for _, bid := range bidsInput {
			if bid.Quantity > 0 {
				bidInt := toInt64(bid.Price)
				if highestBidInt < 0 || bidInt > highestBidInt {
					highestBidInt = bidInt
				}
			}
		}

		for _, ask := range asksInput {
			if ask.Quantity > 0 {
				askInt := toInt64(ask.Price)
				if lowestAskInt < 0 || askInt < lowestAskInt {
					lowestAskInt = askInt
				}
			}
		}

		if highestBidInt >= 0 && lowestAskInt >= 0 && highestBidInt >= lowestAskInt {
			hadCollision = true
		}
	}

	shouldPanic := false
	var panicMsg strings.Builder

	if emptyAsks && hasNonZeroAsks && !hadCollision {
		shouldPanic = true
		panicMsg.WriteString("PANIC: UpdateSnapshot/Delta received non-zero ask quantities but orderbook has no asks after update\n")
	}

	if emptyBids && hasNonZeroBids && !hadCollision {
		shouldPanic = true
		panicMsg.WriteString("PANIC: UpdateSnapshot/Delta received non-zero bid quantities but orderbook has no bids after update\n")
	}

	if shouldPanic {
		panicMsg.WriteString("\n=== Input Data ===\n")
		panicMsg.WriteString(fmt.Sprintf("Asks (%d orders with non-zero: %v):\n", len(asksInput), hasNonZeroAsks))
		for i, order := range asksInput {
			panicMsg.WriteString(fmt.Sprintf("  [%d] Price=%.4f, Quantity=%.4f\n", i, order.Price, order.Quantity))
		}
		panicMsg.WriteString(fmt.Sprintf("\nBids (%d orders with non-zero: %v):\n", len(bidsInput), hasNonZeroBids))
		for i, order := range bidsInput {
			panicMsg.WriteString(fmt.Sprintf("  [%d] Price=%.4f, Quantity=%.4f\n", i, order.Price, order.Quantity))
		}

		panicMsg.WriteString("\n=== Orderbook State ===\n")
		panicMsg.WriteString(fmt.Sprintf("BidHead: %v\n", ob.bidHead != nil))
		if ob.bidHead != nil {
			panicMsg.WriteString(fmt.Sprintf("  Best Bid: Price=%.4f, Quantity=%.4f\n", toFloat64(ob.bidHead.price), ob.bidHead.quantity))
		}
		panicMsg.WriteString(fmt.Sprintf("AskHead: %v\n", ob.askHead != nil))
		if ob.askHead != nil {
			panicMsg.WriteString(fmt.Sprintf("  Best Ask: Price=%.4f, Quantity=%.4f\n", toFloat64(ob.askHead.price), ob.askHead.quantity))
		}

		asks, bids := ob.getDepthInternal(10)
		panicMsg.WriteString(fmt.Sprintf("\nActual depth check: %d asks, %d bids found\n", len(asks), len(bids)))

		panicMsg.WriteString(ob.dumpTreeStructure("BIDS", ob.bids))
		panicMsg.WriteString(ob.dumpTreeStructure("ASKS", ob.asks))

		panic(panicMsg.String())
	}
}
