package main

import (
	"flag"
	"fmt"
	"log"
	"time"
)

// MiniRedis 主入口
// 支持功能：
//   - 内存 KV 存储（LRU 缓存）
//   - RESP 协议兼容
//   - 主从复制
//   - RDB 快照持久化 + AOF 追加日志
//   - 过期键自动清理
//   - 一致性哈希分片
func main() {
	// 命令行参数解析
	port := flag.Int("port", 9000, "TCP服务端口")
	dataDir := flag.String("data", "./data", "数据持久化目录")
	nodeID := flag.String("node-id", "node-1", "集群节点ID")
	isMaster := flag.Bool("master", true, "是否为主节点")
	masterAddr := flag.String("master-addr", "localhost:9000", "主节点地址(从节点使用)")
	rdbInterval := flag.Int("rdb-interval", 60, "RDB快照保存间隔(秒)")
	expireInterval := flag.Int("expire-interval", 10, "过期键清理间隔(秒)")
	enableAOF := flag.Bool("aof", true, "是否启用AOF持久化")
	flag.Parse()

	// 初始化带过期功能的 LRU 缓存，容量 10000
	cache := NewExpireCache(10000)

	// 初始化 AOF 持久化
	aof, err := NewAOF(*dataDir, *enableAOF)
	if err != nil {
		log.Printf("Warning: Failed to create AOF: %v", err)
	}

	// 优先从 AOF 恢复（AOF 数据更新更及时）
	if *enableAOF {
		if err := aof.Restore(cache); err != nil {
			log.Printf("Warning: Failed to load AOF data: %v", err)
			// AOF 恢复失败，尝试从 RDB 恢复
			if err := LoadRDB(cache.cache, *dataDir); err != nil {
				log.Printf("Warning: Failed to load RDB data: %v", err)
			}
		}
	} else {
		// 未启用 AOF，从 RDB 恢复
		if err := LoadRDB(cache.cache, *dataDir); err != nil {
			log.Printf("Warning: Failed to load RDB data: %v", err)
		}
	}

	// 启动过期键清理协程（定时 + 惰性删除）
	go cache.StartExpireLoop(time.Duration(*expireInterval) * time.Second)

	// 启动 RDB 定时快照持久化
	go StartRDBLoop(cache.cache, *dataDir, time.Duration(*rdbInterval)*time.Second)

	// 启动 AOF 重写协程（每 30 秒检查一次）
	if aof != nil {
		go aof.StartRewriteLoop(cache.cache, 30*time.Second)
	}

	// 创建并启动 TCP 服务器
	server := NewTCPServer(*port, cache, *dataDir, *isMaster, *masterAddr, *nodeID, aof)
	fmt.Printf("MiniRedis server started on port %d (role: %s, node-id: %s, AOF: %t)\n",
		*port,
		map[bool]string{true: "master", false: "slave"}[*isMaster],
		*nodeID,
		*enableAOF,
	)

	if err := server.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
