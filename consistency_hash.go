package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// ConsistentHash 一致性哈希算法实现
// 用于分布式集群的请求路由和数据分片
// 原理：将节点和 key 都映射到哈希环上，key 顺时针找到的第一个节点负责处理
type ConsistentHash struct {
	ring     []uint32          // 哈希环（升序排列）
	hashMap  map[uint32]string // 哈希值 -> 节点ID
	replicas int               // 虚拟节点倍数（提高负载均衡性）
	mutex    sync.RWMutex      // 并发访问保护
}

// NewConsistentHash 创建一致性哈希实例
// replicas: 每个物理节点的虚拟节点数量，越多负载越均衡但内存占用越大
func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
}

// hashKey 计算 key 的哈希值（使用 CRC32）
func (c *ConsistentHash) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// AddNode 将节点添加到哈希环
// 每个物理节点对应 replicas 个虚拟节点，均匀分布在环上
func (c *ConsistentHash) AddNode(nodeID string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := 0; i < c.replicas; i++ {
		hash := c.hashKey(fmt.Sprintf("%s%d", nodeID, i))
		c.ring = append(c.ring, hash)
		c.hashMap[hash] = nodeID
	}
	// 保持环的有序性
	sort.Slice(c.ring, func(i, j int) bool { return c.ring[i] < c.ring[j] })
}

// RemoveNode 将节点从哈希环移除
func (c *ConsistentHash) RemoveNode(nodeID string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 删除该节点的所有虚拟节点
	newRing := make([]uint32, 0, len(c.ring))
	for i := 0; i < c.replicas; i++ {
		hash := c.hashKey(fmt.Sprintf("%s%d", nodeID, i))
		delete(c.hashMap, hash)
	}
	// 重建环
	for _, hash := range c.ring {
		if _, exists := c.hashMap[hash]; exists {
			newRing = append(newRing, hash)
		}
	}
	c.ring = newRing
}

// GetNode 根据 key 找到负责的节点
// 使用二分查找找到顺时针方向的第一个节点
func (c *ConsistentHash) GetNode(key string) string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.ring) == 0 {
		return ""
	}

	hash := c.hashKey(key)
	// 二分查找第一个 >= hash 的位置
	idx := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })
	// 如果超过环尾，回绕到环首
	if idx == len(c.ring) {
		idx = 0
	}
	return c.hashMap[c.ring[idx]]
}

// GetNodes 获取所有节点列表
func (c *ConsistentHash) GetNodes() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	seen := make(map[string]bool)
	var nodes []string
	for _, node := range c.hashMap {
		if !seen[node] {
			seen[node] = true
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetReplicas 获取 key 对应的多个节点（用于数据复制）
// 返回 count 个不同的节点，按顺时针顺序
func (c *ConsistentHash) GetReplicas(key string, count int) []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.ring) == 0 {
		return nil
	}

	hash := c.hashKey(key)
	idx := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })

	seen := make(map[string]bool)
	var result []string
	for i := 0; i < len(c.ring) && len(result) < count; i++ {
		pos := (idx + i) % len(c.ring)
		node := c.hashMap[c.ring[pos]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}
