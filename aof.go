package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// AOF 追加式日志持久化
// 原理：每次写操作（SET/DEL）都追加到日志文件
// 恢复：启动时重放日志文件中的所有命令
// 重写：定期基于缓存数据重建 AOF，去除冗余命令
type AOF struct {
	file           *os.File      // AOF 文件句柄
	writer         *bufio.Writer // 缓冲写入器
	mutex          sync.Mutex    // 并发写入保护
	enabled        bool          // 是否启用 AOF
	lastRewrite    time.Time     // 上次重写时间
	rewriteSize    int64         // 上次重写时的文件大小
	minRewriteSize int64         // 最小重写文件大小（字节）
	rewriteRatio   float64       // 文件增长比例触发重写
}

// NewAOF 创建 AOF 持久化实例
// enabled: 是否启用 AOF
// minRewriteSize: 最小重写文件大小（默认 64KB）
// rewriteRatio: 文件增长比例触发重写（默认 100%，即增长一倍）
func NewAOF(dataDir string, enabled bool) (*AOF, error) {
	if !enabled {
		return &AOF{enabled: false}, nil
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	filePath := filepath.Join(dataDir, "appendonly.aof")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// 获取当前文件大小
	stat, _ := file.Stat()
	currentSize := int64(0)
	if stat != nil {
		currentSize = stat.Size()
	}

	return &AOF{
		file:           file,
		writer:         bufio.NewWriter(file),
		enabled:        true,
		lastRewrite:    time.Now(),
		rewriteSize:    currentSize,
		minRewriteSize: 64 * 1024, // 最小 64KB
		rewriteRatio:   1.0,       // 增长 100% 触发重写
	}, nil
}

// Append 追加命令到 AOF 日志
// cmd: 完整的 RESP 格式命令
func (a *AOF) Append(cmd string) error {
	if !a.enabled {
		return nil
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	_, err := a.writer.WriteString(cmd)
	if err != nil {
		return err
	}

	return a.writer.Flush()
}

// Close 关闭 AOF 文件
func (a *AOF) Close() error {
	if !a.enabled {
		return nil
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if err := a.writer.Flush(); err != nil {
		return err
	}
	return a.file.Close()
}

// Restore 从 AOF 文件恢复数据到缓存
func (a *AOF) Restore(cache *ExpireCache) error {
	if !a.enabled {
		return nil
	}

	filePath := a.file.Name()
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	resp := NewRESPParser(file)
	for {
		val, err := resp.Parse()
		if err != nil {
			break
		}

		if val.Type != RESPArray || len(val.Array) == 0 {
			continue
		}

		cmd := string(val.Array[0].Bulk)
		switch cmd {
		case "SET":
			if len(val.Array) >= 3 {
				key := string(val.Array[1].Bulk)
				value := string(val.Array[2].Bulk)
				if len(val.Array) >= 5 && string(val.Array[3].Bulk) == "PX" {
					ms, _ := strconv.ParseInt(string(val.Array[4].Bulk), 10, 64)
					cache.SetWithTTL(key, value, time.Duration(ms)*time.Millisecond)
				} else {
					cache.Set(key, value)
				}
			}
		case "DEL":
			if len(val.Array) >= 2 {
				key := string(val.Array[1].Bulk)
				cache.Delete(key)
			}
		}
	}

	return nil
}

// NeedRewrite 检查是否需要重写 AOF
// 触发条件：
//  1. 文件大小 >= minRewriteSize
//  2. 文件大小 >= rewriteSize * (1 + rewriteRatio)
func (a *AOF) NeedRewrite() bool {
	if !a.enabled {
		return false
	}

	stat, err := a.file.Stat()
	if err != nil {
		return false
	}

	currentSize := stat.Size()
	return currentSize >= a.minRewriteSize &&
		currentSize >= int64(float64(a.rewriteSize)*(1+a.rewriteRatio))
}

// Rewrite 重写 AOF 文件（压缩日志）
// 将当前缓存中的所有数据写入新的 AOF 文件
// 相当于用 RDB 的快照思想来压缩 AOF
func (a *AOF) Rewrite(cache *LRUCache) error {
	if !a.enabled {
		return nil
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	dataDir := filepath.Dir(a.file.Name())
	tempPath := filepath.Join(dataDir, "appendonly.aof.tmp")

	tempFile, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	data := cache.GetAll()

	for key, value := range data {
		cmd := EncodeArray([]RESPValue{
			EncodeBulk([]byte("SET")),
			EncodeBulk([]byte(key)),
			EncodeBulk([]byte(value)),
		})
		writer.WriteString(cmd.String())
	}

	writer.Flush()

	// 原子替换原文件
	if err := os.Rename(tempPath, a.file.Name()); err != nil {
		return err
	}

	// 更新重写记录
	a.lastRewrite = time.Now()
	stat, _ := a.file.Stat()
	if stat != nil {
		a.rewriteSize = stat.Size()
	}

	return nil
}

// StartRewriteLoop 启动定时重写协程
// 定期检查并执行 AOF 重写
func (a *AOF) StartRewriteLoop(cache *LRUCache, interval time.Duration) {
	if !a.enabled {
		return
	}

	ticker := time.NewTicker(interval)
	for range ticker.C {
		if a.NeedRewrite() {
			a.Rewrite(cache)
		}
	}
}

// GetFileSize 获取当前 AOF 文件大小
func (a *AOF) GetFileSize() int64 {
	if !a.enabled {
		return 0
	}
	stat, err := a.file.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
}
