package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TCPServer TCP 服务器
// 支持 RESP 协议的 Redis 兼容服务
// 包含：主从复制、一致性哈希、过期键清理、AOF 持久化
type TCPServer struct {
	port        int             // 监听端口
	cache       *ExpireCache    // 带过期功能的缓存
	dataDir     string          // 持久化数据目录
	replication *Replication    // 主从复制模块
	hashRing    *ConsistentHash // 一致性哈希环
	nodeID      string          // 节点唯一标识
	isMaster    bool            // 是否为主节点
	mutex       sync.Mutex      // 服务器级别的锁
	shutdown    bool            // 关闭标志
	aof         *AOF            // AOF 追加日志持久化
}

// NewTCPServer 创建 TCP 服务器实例
// 参数：端口、缓存、数据目录、是否主节点、主节点地址（从节点用）、节点ID、AOF实例
func NewTCPServer(port int, cache *ExpireCache, dataDir string, isMaster bool, masterAddr string, nodeID string, aof *AOF) *TCPServer {
	s := &TCPServer{
		port:     port,
		cache:    cache,
		dataDir:  dataDir,
		isMaster: isMaster,
		nodeID:   nodeID,
		hashRing: NewConsistentHash(100), // 100 个虚拟节点
		aof:      aof,
	}

	// 主节点初始化复制模块，从节点启动同步协程
	if isMaster {
		s.replication = NewReplication(true, "")
	} else {
		s.replication = NewReplication(false, masterAddr)
		go s.replication.StartSlaveSync(cache.cache)
	}

	// 将自身节点加入哈希环
	s.hashRing.AddNode(nodeID)

	return s
}

// AddClusterNode 添加集群节点到哈希环
func (s *TCPServer) AddClusterNode(nodeID string) {
	s.hashRing.AddNode(nodeID)
}

// RemoveClusterNode 从哈希环移除节点
func (s *TCPServer) RemoveClusterNode(nodeID string) {
	s.hashRing.RemoveNode(nodeID)
}

// Start 启动 TCP 服务器，监听并处理连接
func (s *TCPServer) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.shutdown {
				return nil
			}
			continue
		}
		// 每个连接单独 goroutine 处理
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个客户端连接
// 循环读取 RESP 格式命令并处理，直到连接关闭
func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	resp := NewRESPParser(conn)
	for {
		val, err := resp.Parse()
		fmt.Printf("get is: %q\n", val)
		if err != nil {
			return // 连接断开或解析错误
		}

		// 只处理数组类型命令（如 GET key, SET key value）
		if val.Type != RESPArray || len(val.Array) == 0 {
			continue
		}

		// 命令名转大写，参数为剩余元素
		cmd := strings.ToUpper(string(val.Array[0].Bulk))
		result := s.handleCommand(cmd, val.Array[1:])
		conn.Write([]byte(result.String()))
	}
}

// handleCommand 根据命令类型分发处理
func (s *TCPServer) handleCommand(cmd string, args []RESPValue) RESPValue {
	switch cmd {
	case "GET":
		return s.handleGet(args)
	case "SET":
		return s.handleSet(args)
	case "DEL":
		return s.handleDel(args)
	case "PING":
		return EncodeString("PONG")
	case "QUIT":
		return EncodeString("OK")
	case "INFO":
		return s.handleInfo()
	case "SYNC":
		return s.handleSync()
	case "REPLICAOF":
		return s.handleReplicaOf(args)
	case "TTL":
		return s.handleTTL(args)
	case "CLUSTER":
		return s.handleCluster(args)
	default:
		return EncodeError("unknown command '" + cmd + "'")
	}
}

// ===== 命令处理器 =====

// handleGet 获取指定 key 的值
// 返回值格式：$length\r\nvalue\r\n 或 $-1\r\n（不存在）
func (s *TCPServer) handleGet(args []RESPValue) RESPValue {
	if len(args) != 1 {
		return EncodeError("wrong number of arguments")
	}

	key := string(args[0].Bulk)
	value, ok := s.cache.Get(key)
	if ok {
		return EncodeBulk([]byte(value))
	}
	return EncodeBulk(nil)
}

// handleSet 设置 key-value 对，支持 PX 参数设置过期时间（毫秒）
// 主节点会同步广播到所有从节点
func (s *TCPServer) handleSet(args []RESPValue) RESPValue {
	if len(args) < 2 {
		return EncodeError("wrong number of arguments")
	}

	key := string(args[0].Bulk)
	value := string(args[1].Bulk)

	// 解析可选的过期时间参数：SET key value PX milliseconds
	var ttl time.Duration
	if len(args) >= 4 && strings.ToUpper(string(args[2].Bulk)) == "PX" {
		ms, err := strconv.ParseInt(string(args[3].Bulk), 10, 64)
		if err == nil {
			ttl = time.Duration(ms) * time.Millisecond
		}
	}

	// 写入缓存
	if ttl > 0 {
		s.cache.SetWithTTL(key, value, ttl)
	} else {
		s.cache.Set(key, value)
	}

	// 构建 RESP 命令（用于 AOF 和主从同步）
	respArgs := []RESPValue{
		EncodeBulk([]byte("SET")),
		EncodeBulk([]byte(key)),
		EncodeBulk([]byte(value)),
	}
	if ttl > 0 {
		respArgs = append(respArgs,
			EncodeBulk([]byte("PX")),
			EncodeBulk([]byte(strconv.FormatInt(ttl.Milliseconds(), 10))),
		)
	}
	respCmd := EncodeArray(respArgs)

	// 追加到 AOF 日志
	if s.aof != nil {
		s.aof.Append(respCmd.String())
	}

	// 主节点同步命令到所有从节点
	if s.isMaster && s.replication != nil {
		s.replication.SyncCommand(&respCmd)
	}

	return EncodeString("OK")
}

// handleDel 删除指定 key
// 返回删除的数量（1 或 0）
func (s *TCPServer) handleDel(args []RESPValue) RESPValue {
	if len(args) != 1 {
		return EncodeError("wrong number of arguments")
	}

	key := string(args[0].Bulk)
	success := s.cache.Delete(key)

	// 构建 RESP 命令（用于 AOF 和主从同步）
	respCmd := EncodeArray([]RESPValue{
		EncodeBulk([]byte("DEL")),
		EncodeBulk([]byte(key)),
	})

	// 追加到 AOF 日志
	if s.aof != nil {
		s.aof.Append(respCmd.String())
	}

	// 主节点同步删除命令到从节点
	if s.isMaster && s.replication != nil {
		s.replication.SyncCommand(&respCmd)
	}

	if success {
		return EncodeInteger(1)
	}
	return EncodeInteger(0)
}

// handleInfo 返回服务器信息
// 包含：角色（master/slave）、节点ID、端口
func (s *TCPServer) handleInfo() RESPValue {
	info := fmt.Sprintf("role:%s\nnode_id:%s\nport:%d",
		map[bool]string{true: "master", false: "slave"}[s.isMaster],
		s.nodeID,
		s.port,
	)
	return EncodeBulk([]byte(info))
}

// handleSync 处理 SYNC 命令（主从同步）
func (s *TCPServer) handleSync() RESPValue {
	return EncodeString("SYNC not implemented")
}

// handleReplicaOf 处理 REPLICAOF 命令（切换主节点）
func (s *TCPServer) handleReplicaOf(args []RESPValue) RESPValue {
	return EncodeError("REPLICAOF not implemented")
}

// handleTTL 返回 key 的剩余生存时间（毫秒）
// -1 表示永久存在，-2 表示不存在
func (s *TCPServer) handleTTL(args []RESPValue) RESPValue {
	if len(args) != 1 {
		return EncodeError("wrong number of arguments")
	}

	key := string(args[0].Bulk)
	ttl := s.cache.TTL(key)
	if ttl < 0 {
		return EncodeInteger(-1)
	}
	return EncodeInteger(int64(ttl / time.Millisecond))
}

// handleCluster 处理集群相关命令
// CLUSTER NODES - 获取所有节点
// CLUSTER ADD node-id - 添加节点
// CLUSTER REMOVE node-id - 移除节点
func (s *TCPServer) handleCluster(args []RESPValue) RESPValue {
	if len(args) == 0 {
		return EncodeError("wrong number of arguments")
	}

	subCmd := strings.ToUpper(string(args[0].Bulk))
	switch subCmd {
	case "NODES":
		nodes := s.hashRing.GetNodes()
		var respArgs []RESPValue
		for _, node := range nodes {
			respArgs = append(respArgs, EncodeBulk([]byte(node)))
		}
		return EncodeArray(respArgs)
	case "ADD":
		if len(args) != 2 {
			return EncodeError("wrong number of arguments")
		}
		nodeID := string(args[1].Bulk)
		s.hashRing.AddNode(nodeID)
		return EncodeString("OK")
	case "REMOVE":
		if len(args) != 2 {
			return EncodeError("wrong number of arguments")
		}
		nodeID := string(args[1].Bulk)
		s.hashRing.RemoveNode(nodeID)
		return EncodeString("OK")
	default:
		return EncodeError("unknown cluster command")
	}
}
