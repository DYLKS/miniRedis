package main

import (
	"sync"
)

// LRUCache 基于双向链表 + HashMap 的 LRU 缓存实现
// 结构：HashMap 提供 O(1) 查找，双向链表维护访问顺序
// 优化：使用 freeList 避免频繁内存申请释放
// 淘汰策略：容量超限时删除链表尾部（最久未使用）
type LRUCache struct {
	capacity int              // 缓存最大容量
	cache    map[string]*Node // HashMap: key -> 链表节点
	head     *Node            // 双向链表头节点（哨兵，不存储真实数据）
	tail     *Node            // 双向链表尾节点（哨兵）
	freeList *Node            // 空闲节点链表
	mutex    sync.RWMutex     // 读写锁，支持并发访问
}

// Node 双向链表节点
// 通过 prev/next 形成链表，head -> ... -> tail
// head 的 next 是最近使用的节点，tail 的 prev 是最久未使用的节点
type Node struct {
	key   string // 用于淘汰时从 map 中删除
	value string // 存储的值
	prev  *Node  // 前驱节点
	next  *Node  // 后继节点
}

// NewLRUCache 创建指定容量的 LRU 缓存
func NewLRUCache(capacity int) *LRUCache {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.prev = head

	c := &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*Node),
		head:     head,
		tail:     tail,
		freeList: &Node{}, // 哨兵节点
	}

	// 预分配一些节点到 freeList
	c.initFreeList()
	return c
}

// initFreeList 预分配空闲节点
func (c *LRUCache) initFreeList() {
	// 预分配一半容量的节点到 freeList
	preAlloc := c.capacity / 2
	if preAlloc < 10 {
		preAlloc = 10
	}

	for i := 0; i < preAlloc; i++ {
		node := &Node{}
		c.addToFreeList(node)
	}
}

// Get 根据 key 获取值，并将该节点移到链表头部（最近使用）
// 返回值和是否存在该 key
func (c *LRUCache) Get(key string) (string, bool) {
	c.mutex.RLock()
	node, ok := c.cache[key]
	if !ok {
		c.mutex.RUnlock()
		return "", false
	}
	val := node.value
	c.mutex.RUnlock()

	c.mutex.Lock()
	c.moveToHead(node)
	c.mutex.Unlock()

	return val, true
}

// Set 设置 key-value，若 key 已存在则更新值并移到头部
// 若不存在则插入头部，超容量时淘汰尾部节点
func (c *LRUCache) Set(key, value string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if node, ok := c.cache[key]; ok {
		// 键已存在，更新值并移到链表头部
		node.value = value
		c.moveToHead(node)
		return
	}

	// 键不存在，从 freeList 获取或创建新节点
	newNode := c.getNodeFromFreeList()
	if newNode == nil {
		newNode = &Node{}
	}
	newNode.key = key
	newNode.value = value

	c.cache[key] = newNode
	c.addToHead(newNode)

	// 检查是否超过容量，超出则淘汰尾部节点
	if len(c.cache) > c.capacity {
		tailNode := c.removeTail()
		delete(c.cache, tailNode.key)
		// 将被淘汰的节点放回 freeList
		c.addToFreeList(tailNode)
	}
}

// Delete 删除指定 key，从链表和 map 中移除
func (c *LRUCache) Delete(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if node, ok := c.cache[key]; ok {
		c.removeNode(node)
		delete(c.cache, key)
		// 将被删除的节点放回 freeList
		c.addToFreeList(node)
		return true
	}
	return false
}

// GetAll 获取所有键值对，用于持久化存储
func (c *LRUCache) GetAll() map[string]string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make(map[string]string)
	for key, node := range c.cache {
		result[key] = node.value
	}
	return result
}

// ===== 双向链表操作 =====

// moveToHead 将节点移到链表头部
func (c *LRUCache) moveToHead(node *Node) {
	c.removeNode(node)
	c.addToHead(node)
}

// addToHead 将节点添加到链表头部
func (c *LRUCache) addToHead(node *Node) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

// removeNode 从双向链表中移除指定节点
func (c *LRUCache) removeNode(node *Node) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

// removeTail 移除链表尾部节点（最久未使用）
// 返回被移除的节点
func (c *LRUCache) removeTail() *Node {
	tailNode := c.tail.prev
	c.removeNode(tailNode)
	return tailNode
}

// ===== FreeList 操作 =====

// getNodeFromFreeList 从 freeList 获取一个空闲节点
func (c *LRUCache) getNodeFromFreeList() *Node {
	if c.freeList.next == nil {
		return nil
	}
	node := c.freeList.next
	c.freeList.next = node.next

	if c.freeList.next != nil {
		c.freeList.next.prev = c.freeList
	}
	// 清除节点引用，避免内存泄漏
	node.prev = nil
	node.next = nil
	return node
}

// addToFreeList 将节点放回 freeList
func (c *LRUCache) addToFreeList(node *Node) {
	// 清除旧数据
	node.key = ""
	node.value = ""
	// 添加到 freeList 头部
	node.next = c.freeList.next
	node.prev = c.freeList
	if c.freeList.next != nil {
		c.freeList.next.prev = node
	}
	c.freeList.next = node
}
