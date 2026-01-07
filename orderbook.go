package orderbook

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
)

// Epsilon для сравнения float64
const epsilon = 1e-9

// Степень B-дерева (минимальное количество ключей в узле = btreeDegree-1)
const btreeDegree = 32

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

// floatEquals сравнивает два float64 с учётом погрешности
func floatEquals(a, b float64) bool {
	return math.Abs(a-b) < epsilon
}

// floatLess проверяет a < b с учётом погрешности
func floatLess(a, b float64) bool {
	return a < b-epsilon
}

// floatGreater проверяет a > b с учётом погрешности
func floatGreater(a, b float64) bool {
	return a > b+epsilon
}

// floatGreaterOrEqual проверяет a >= b с учётом погрешности
func floatGreaterOrEqual(a, b float64) bool {
	return a >= b-epsilon
}

// floatLessOrEqual проверяет a <= b с учётом погрешности
func floatLessOrEqual(a, b float64) bool {
	return a <= b+epsilon
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

	count := 0
	curr := ob.bidHead
	for curr != nil && count < ob.maxDepth {
		count++
		curr = curr.prev
	}
	for curr != nil {
		toRemove := curr
		curr = curr.prev
		ob.removeBid(toRemove.price)
	}

	count = 0
	curr = ob.askHead
	for curr != nil && count < ob.maxDepth {
		count++
		curr = curr.next
	}
	for curr != nil {
		toRemove := curr
		curr = curr.next
		ob.removeAsk(toRemove.price)
	}
}

func (ob *OrderBook) UpdateSnapshot(asks, bids []Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// Bids processing
	if len(bids) > 0 {
		highestBid := bids[0].Price
		for _, bid := range bids {
			if floatGreater(bid.Price, highestBid) {
				highestBid = bid.Price
			}
		}

		toRemove := make([]float64, 0)
		curr := ob.bidHead
		for curr != nil && floatGreater(curr.price, highestBid) {
			toRemove = append(toRemove, curr.price)
			curr = curr.prev
		}
		for _, p := range toRemove {
			ob.removeBid(p)
		}

		for _, order := range bids {
			if floatGreater(order.Quantity, 0) {
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
			if floatLess(ask.Price, lowestAsk) {
				lowestAsk = ask.Price
			}
		}

		toRemove := make([]float64, 0)
		curr := ob.askHead
		for curr != nil && floatLess(curr.price, lowestAsk) {
			toRemove = append(toRemove, curr.price)
			curr = curr.next
		}
		for _, p := range toRemove {
			ob.removeAsk(p)
		}

		for _, order := range asks {
			if floatGreater(order.Quantity, 0) {
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

	// Исправление пересечений bid >= ask (symmetric fix for both sides)
	// Iterate until collision is resolved or one side is empty
	for ob.bidHead != nil && ob.askHead != nil && floatGreaterOrEqual(ob.bidHead.price, ob.askHead.price) {
		// Remove bids >= lowest ask
		toRemove := make([]float64, 0)
		curr := ob.bidHead
		for curr != nil && floatGreaterOrEqual(curr.price, ob.askHead.price) {
			toRemove = append(toRemove, curr.price)
			curr = curr.prev
		}
		for _, p := range toRemove {
			ob.removeBid(p)
		}
		ob.bidHead = ob.findHighestBid(ob.bids)

		// Also remove asks <= highest bid (symmetric fix)
		if ob.bidHead != nil && ob.askHead != nil && floatGreaterOrEqual(ob.bidHead.price, ob.askHead.price) {
			toRemove = make([]float64, 0)
			curr = ob.askHead
			for curr != nil && floatLessOrEqual(curr.price, ob.bidHead.price) {
				toRemove = append(toRemove, curr.price)
				curr = curr.next
			}
			for _, p := range toRemove {
				ob.removeAsk(p)
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
	return ob.getDepthInternal(levels)
}

// getDepthInternal is the internal version of GetDepth without locking
// Used when we already hold the lock (e.g., during validation)
func (ob *OrderBook) getDepthInternal(levels int) (asks, bids []Order) {
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

func (ob *OrderBook) GetSpread() (spread float64, ok bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.bidHead == nil || ob.askHead == nil {
		return 0, false
	}
	return ob.askHead.price - ob.bidHead.price, true
}

func (ob *OrderBook) GetVersion() uint64 {
	return atomic.LoadUint64(&ob.version)
}

func (ob *OrderBook) insertBid(price float64, quantity float64) {
	level := ob.searchOrInsert(ob.bids, price, true)
	if level != nil {
		level.quantity = quantity
	}
}

func (ob *OrderBook) insertAsk(price float64, quantity float64) {
	level := ob.searchOrInsert(ob.asks, price, false)
	if level != nil {
		level.quantity = quantity
	}
}

func (ob *OrderBook) updateBid(price float64, quantity float64) {
	if floatEquals(quantity, 0) {
		ob.removeBid(price)
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
	if floatEquals(quantity, 0) {
		ob.removeAsk(price)
	} else {
		level := ob.findAsk(price)
		if level != nil {
			level.quantity = quantity
		} else {
			ob.insertAsk(price, quantity)
		}
	}
}

func (ob *OrderBook) removeBid(price float64) {
	ob.removeFromTree(ob.bids, price, true)
	ob.bidHead = ob.findHighestBid(ob.bids)
}

func (ob *OrderBook) removeAsk(price float64) {
	ob.removeFromTree(ob.asks, price, false)
	ob.askHead = ob.findLowestAsk(ob.asks)
}

func (ob *OrderBook) findBid(price float64) *priceLevel {
	return ob.search(ob.bids, price)
}

func (ob *OrderBook) findAsk(price float64) *priceLevel {
	return ob.search(ob.asks, price)
}

// search ищет priceLevel по цене в B-дереве
func (ob *OrderBook) search(n *bnode, price float64) *priceLevel {
	if n == nil {
		return nil
	}

	for {
		idx := ob.findKeyIndex(n.prices, price)

		if idx < len(n.prices) && floatEquals(n.prices[idx], price) {
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
func (ob *OrderBook) findKeyIndex(prices []float64, price float64) int {
	lo, hi := 0, len(prices)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if floatLess(prices[mid], price) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// searchOrInsert ищет или вставляет priceLevel
func (ob *OrderBook) searchOrInsert(root *bnode, price float64, isBid bool) *priceLevel {
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

	return ob.insertNonFull(root, price, isBid)
}

// insertNonFull вставляет в узел, который гарантированно не полон
func (ob *OrderBook) insertNonFull(n *bnode, price float64, isBid bool) *priceLevel {
	for {
		idx := ob.findKeyIndex(n.prices, price)

		// Если ключ уже существует
		if idx < len(n.prices) && floatEquals(n.prices[idx], price) {
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
			level := &priceLevel{price: price}
			ob.insertIntoLeafAt(n, idx, price, level)
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
			if floatGreater(price, n.prices[idx]) {
				idx++
			} else if floatEquals(price, n.prices[idx]) {
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
func (ob *OrderBook) insertIntoLeafAt(n *bnode, idx int, price float64, level *priceLevel) {
	// Расширяем слайсы
	n.prices = append(n.prices, 0)
	copy(n.prices[idx+1:], n.prices[idx:])
	n.prices[idx] = price

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
		prices: make([]float64, newPricesLen),
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
func (ob *OrderBook) removeFromTree(root *bnode, price float64, isBid bool) {
	if root == nil || (len(root.prices) == 0 && len(root.children) == 0) {
		return
	}

	ob.removeRecursive(root, price, isBid)

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

func (ob *OrderBook) removeRecursive(n *bnode, price float64, isBid bool) bool {
	idx := ob.findKeyIndex(n.prices, price)

	if n.isLeaf {
		if idx < len(n.prices) && floatEquals(n.prices[idx], price) {
			ob.removeFromLeafAt(n, idx)
			return true
		}
		return false
	}

	// Внутренний узел
	// В B+ дереве, если ключ совпадает с separator в внутреннем узле,
	// продолжаем поиск в правом поддереве, так как данные находятся только в листьях
	if idx < len(n.prices) && floatEquals(n.prices[idx], price) {
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
		ob.ensureMinKeys(n, idx) // <- убрали isBid
		// После балансировки нужно пересчитать индекс и проверить, не переместился ли ключ
		idx = ob.findKeyIndex(n.prices, price)

		// Ключ мог переместиться в текущий узел после балансировки
		if idx < len(n.prices) && floatEquals(n.prices[idx], price) {
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

	return ob.removeRecursive(child, price, isBid)
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

		child.prices = append([]float64{lastPrice}, child.prices...)
		child.levels = append([]*priceLevel{lastLevel}, child.levels...)

		leftSibling.prices = leftSibling.prices[:len(leftSibling.prices)-1]
		leftSibling.levels = leftSibling.levels[:len(leftSibling.levels)-1]

		parent.prices[childIdx-1] = child.prices[0]
	} else {
		child.prices = append([]float64{parent.prices[childIdx-1]}, child.prices...)

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
	atomic.AddUint64(&ob.version, 1)
}

func (ob *OrderBook) IsEmpty() bool {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	return ob.bidHead == nil && ob.askHead == nil
}

func hasNonZeroQuantity(orders []Order) bool {
	for _, order := range orders {
		if floatGreater(order.Quantity, 0) {
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
				sb.WriteString(fmt.Sprintf("%.2f:%.4f", level.price, level.quantity))
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
		highestBid := -1.0
		lowestAsk := -1.0

		for _, bid := range bidsInput {
			if floatGreater(bid.Quantity, 0) && (highestBid < 0 || floatGreater(bid.Price, highestBid)) {
				highestBid = bid.Price
			}
		}

		for _, ask := range asksInput {
			if floatGreater(ask.Quantity, 0) && (lowestAsk < 0 || floatLess(ask.Price, lowestAsk)) {
				lowestAsk = ask.Price
			}
		}

		if highestBid >= 0 && lowestAsk >= 0 && floatGreaterOrEqual(highestBid, lowestAsk) {
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
			panicMsg.WriteString(fmt.Sprintf("  Best Bid: Price=%.4f, Quantity=%.4f\n", ob.bidHead.price, ob.bidHead.quantity))
		}
		panicMsg.WriteString(fmt.Sprintf("AskHead: %v\n", ob.askHead != nil))
		if ob.askHead != nil {
			panicMsg.WriteString(fmt.Sprintf("  Best Ask: Price=%.4f, Quantity=%.4f\n", ob.askHead.price, ob.askHead.quantity))
		}

		asks, bids := ob.getDepthInternal(10)
		panicMsg.WriteString(fmt.Sprintf("\nActual depth check: %d asks, %d bids found\n", len(asks), len(bids)))

		panicMsg.WriteString(ob.dumpTreeStructure("BIDS", ob.bids))
		panicMsg.WriteString(ob.dumpTreeStructure("ASKS", ob.asks))

		panic(panicMsg.String())
	}
}
