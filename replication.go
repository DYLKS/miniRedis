package main

import (
	"bufio"
	"net"
	"sync"
	"time"
)

// Replication 主从复制模块
// 实现：主节点广播写命令到所有从节点，从节点异步接收并执行
// 机制：主节点写操作后通过 channel 异步广播，从节点维护长连接同步
type Replication struct {
	isMaster   bool                  // 是否为主节点
	masterAddr string                // 主节点地址（从节点使用）
	slaves     map[string]*slaveConn // 从节点连接集合
	mutex      sync.RWMutex          // 读写锁
	syncChan   chan *RESPValue       // 命令广播通道（主节点 -> 从节点）
}

// slaveConn 从节点连接信息
type slaveConn struct {
	conn   net.Conn // TCP 连接
	active bool     // 连接是否活跃
}

// NewReplication 创建复制模块
// 主节点：启动 syncLoop 协程处理命令广播
// 从节点：不启动 syncLoop，通过 StartSlaveSync 协程接收同步
func NewReplication(isMaster bool, masterAddr string) *Replication {
	r := &Replication{
		isMaster:   isMaster,
		masterAddr: masterAddr,
		slaves:     make(map[string]*slaveConn),
		syncChan:   make(chan *RESPValue, 1000), // 缓冲队列防止阻塞
	}
	if isMaster {
		go r.syncLoop() // 主节点启动命令广播循环
	}
	return r
}

// AddSlave 添加从节点连接
func (r *Replication) AddSlave(conn net.Conn) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	addr := conn.RemoteAddr().String()
	r.slaves[addr] = &slaveConn{conn: conn, active: true}
}

// RemoveSlave 移除从节点连接
func (r *Replication) RemoveSlave(addr string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if slave, ok := r.slaves[addr]; ok {
		slave.conn.Close()
		delete(r.slaves, addr)
	}
}

// syncLoop 主节点命令广播循环
// 从 syncChan 接收命令，并发发送到所有从节点
// 发送失败的连接标记为 inactive
func (r *Replication) syncLoop() {
	for cmd := range r.syncChan {
		r.mutex.RLock()
		for addr, slave := range r.slaves {
			if !slave.active {
				continue
			}
			go func(conn net.Conn, address string) {
				_, err := conn.Write([]byte(cmd.String()))
				if err != nil {
					// 发送失败，标记连接为 inactive
					r.mutex.Lock()
					slave.active = false
					r.mutex.Unlock()
				}
			}(slave.conn, addr)
		}
		r.mutex.RUnlock()
	}
}

// SyncCommand 主节点调用此方法广播命令到所有从节点
// 使用非阻塞发送，队列满时丢弃命令
func (r *Replication) SyncCommand(cmd *RESPValue) {
	if !r.isMaster {
		return
	}
	select {
	case r.syncChan <- cmd:
	default:
		// 队列满，丢弃命令（可优化为阻塞或扩容）
	}
}

// StartSlaveSync 从节点同步协程
// 持续连接主节点，接收全量同步后进入增量同步模式
func (r *Replication) StartSlaveSync(cache *LRUCache) error {
	if r.isMaster {
		return nil
	}

	for {
		// 连接主节点
		conn, err := net.Dial("tcp", r.masterAddr)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// 发送 SYNC 请求
		_, err = conn.Write([]byte("SYNC\r\n"))
		if err != nil {
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 等待 SYNC_COMPLETE 标记（全量同步完成）
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "SYNC_COMPLETE" {
				break
			}
		}

		// 全量同步完成，进入增量同步阶段
		resp := NewRESPParser(conn)
		for {
			val, err := resp.Parse()
			if err != nil {
				break // 连接断开，重连
			}
			// 解析并执行命令
			if val.Type == RESPArray && len(val.Array) >= 2 {
				cmd := string(val.Array[0].Bulk)
				if cmd == "SET" && len(val.Array) >= 3 {
					key := string(val.Array[1].Bulk)
					value := string(val.Array[2].Bulk)
					cache.Set(key, value)
				} else if cmd == "DEL" && len(val.Array) >= 2 {
					key := string(val.Array[1].Bulk)
					cache.Delete(key)
				}
			}
		}
		conn.Close()
		time.Sleep(1 * time.Second)
	}
}

// HandleSyncCommand 主节点处理从节点的 SYNC 请求
// 发送全量数据后，将从节点加入监听列表
func (r *Replication) HandleSyncCommand(conn net.Conn, cache *LRUCache) {
	conn.Write([]byte("+SYNC_START\r\n"))

	// 发送所有现有数据
	data := cache.GetAll()
	for key, value := range data {
		resp := EncodeArray([]RESPValue{
			EncodeBulk([]byte("SET")),
			EncodeBulk([]byte(key)),
			EncodeBulk([]byte(value)),
		})
		conn.Write([]byte(resp.String()))
	}

	// 发送同步完成标记
	conn.Write([]byte("+SYNC_COMPLETE\r\n"))

	// 将从节点加入监听列表
	addr := conn.RemoteAddr().String()
	r.AddSlave(conn)

	// 保持连接，接收后续增量命令
	defer r.RemoveSlave(addr)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		// 保持长连接，持续接收增量同步
	}
}
