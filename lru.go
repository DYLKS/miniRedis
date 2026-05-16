package main

import (
	"sync"
)

// LRUCache 基于双向链表 + HashMap 的 LRU 缓存实现
// 结构：HashMap 提供 O(1) 查找，双向链表维护访问顺序
// 淘汰策略：容量超限时删除链表尾部（最久未使用）
type LRUCache struct {
	capacity int              // 缓存最大容量
	cache    map[string]*Node // HashMap: key -> 链表节点
	head     *Node            // 双向链表头节点（哨兵，不存储真实数据）
	tail     *Node            // 双向链表尾节点（哨兵）
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
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*Node),
		head:     &Node{}, // 哨兵节点
		tail:     &Node{}, // 哨兵节点
	}
}

// init 初始化双向链表（仅在首次添加元素时调用）
func (c *LRUCache) init() {
	if c.head.next == nil {
		c.head.next = c.tail
		c.tail.prev = c.head
	}
}

// Get 根据 key 获取值，并将该节点移到链表头部（最近使用）
// 返回值和是否存在该 key
func (c *LRUCache) Get(key string) (string, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if node, ok := c.cache[key]; ok {
		// 访问后移到头部
		c.moveToHead(node)
		return node.value, true
	}
	return "", false
}

// Set 设置 key-value，若 key 已存在则更新值并移到头部
// 若不存在则插入头部，超容量时淘汰尾部节点
func (c *LRUCache) Set(key, value string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.init()

	if node, ok := c.cache[key]; ok {
		// 键已存在，更新值并移到链表头部
		node.value = value
		c.moveToHead(node)
		return
	}

	// 键不存在，创建新节点插入链表头部
	newNode := &Node{key: key, value: value}
	c.cache[key] = newNode
	c.addToHead(newNode)

	// 检查是否超过容量，超出则淘汰尾部节点
	if len(c.cache) > c.capacity {
		tailNode := c.removeTail()
		delete(c.cache, tailNode.key)
	}
}

// Delete 删除指定 key，从链表和 map 中移除
func (c *LRUCache) Delete(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if node, ok := c.cache[key]; ok {
		c.removeNode(node)
		delete(c.cache, key)
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
