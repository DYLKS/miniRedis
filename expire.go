package main

import (
	"sync"
	"time"
)

// ExpireCache 带过期功能的缓存包装器
// 组合 LRU 缓存 + 过期时间映射
// 过期策略：惰性删除（Get时检查）+ 定时清理
type ExpireCache struct {
	cache   *LRUCache        // 底层 LRU 缓存
	expires map[string]int64 // key -> 过期时间（纳秒时间戳）
	mutex   sync.RWMutex     // 读写锁
}

// NewExpireCache 创建带过期功能的缓存
func NewExpireCache(capacity int) *ExpireCache {
	return &ExpireCache{
		cache:   NewLRUCache(capacity),
		expires: make(map[string]int64),
	}
}

// Get 获取值，支持惰性删除
// 如果 key 已过期，在获取前先删除
func (e *ExpireCache) Get(key string) (string, bool) {
	e.mutex.RLock()
	expireAt, exists := e.expires[key]
	if exists && expireAt > 0 && time.Now().UnixNano() > expireAt {
		// 已过期，先删除再返回不存在
		e.mutex.RUnlock()
		e.Delete(key)
		return "", false
	}
	e.mutex.RUnlock()

	return e.cache.Get(key)
}

// Set 设置值（无过期时间）
func (e *ExpireCache) Set(key, value string) {
	e.cache.Set(key, value)
}

// SetWithTTL 设置值并指定过期时间
// ttl <= 0 表示永不过期
func (e *ExpireCache) SetWithTTL(key, value string, ttl time.Duration) {
	e.mutex.Lock()
	e.cache.Set(key, value)
	if ttl > 0 {
		// 计算过期时间戳（纳秒）
		e.expires[key] = time.Now().UnixNano() + ttl.Nanoseconds()
	} else {
		delete(e.expires, key)
	}
	e.mutex.Unlock()
}

// Delete 删除键（同时删除过期时间和缓存数据）
func (e *ExpireCache) Delete(key string) bool {
	e.mutex.Lock()
	delete(e.expires, key)
	e.mutex.Unlock()
	return e.cache.Delete(key)
}

// GetAll 获取所有非过期的键值对
func (e *ExpireCache) GetAll() map[string]string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	result := make(map[string]string)
	now := time.Now().UnixNano()

	for key, value := range e.cache.GetAll() {
		expireAt, exits := e.expires[key]
		if !exits || expireAt <= 0 || now <= expireAt {
			result[key] = value
		}
	}
	return result
}

// StartExpireLoop 启动定时过期清理协程
// 定期检查并删除所有已过期的键
func (e *ExpireCache) StartExpireLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		e.cleanExpired()
	}
}

// cleanExpired 清理所有已过期的键
func (e *ExpireCache) cleanExpired() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	now := time.Now().UnixNano()
	for key, expireAt := range e.expires {
		if expireAt > 0 && now > expireAt {
			delete(e.expires, key)
			e.cache.Delete(key)
		}
	}
}

// TTL 返回 key 的剩余生存时间
// 返回值：
//
//	-1: key 不存在或永不过期
//	 0: key 已过期
//	>0: 剩余时间（纳秒）
func (e *ExpireCache) TTL(key string) time.Duration {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	expireAt, exists := e.expires[key]
	if !exists || expireAt <= 0 {
		return -1
	}

	remaining := expireAt - time.Now().UnixNano()
	if remaining <= 0 {
		return 0
	}
	return time.Duration(remaining)
}
