# MiniRedis

学习项目，一个用 Go 语言实现的迷你分布式缓存系统，具备 Redis 的核心功能特性。

## 功能特性

### 核心功能
- **内存 KV 存储**：基于 LRU 算法的高效内存缓存
- **RESP 协议**：兼容 Redis 序列化协议，支持 redis-cli 直接连接
- **过期键管理**：支持惰性删除和定时清理
- **持久化**：AOF 追加日志 + RDB 快照
- **主从复制**：支持主节点广播写命令到从节点
- **一致性哈希**：支持集群节点动态增删和负载均衡

### 性能优化
- **LRU freeList**：节点复用避免频繁内存申请释放
- **并发安全**：使用读写锁保证线程安全
- **AOF 重写**：智能压缩日志文件

## 技术栈

- **语言**：Go 1.21+
- **网络**：TCP 协议
- **数据结构**：双向链表 + HashMap
- **并发控制**：sync.RWMutex

## 项目结构

```
.
├── main.go              # 程序入口
├── server.go            # TCP 服务器
├── resp.go              # RESP 协议解析
├── lru.go               # LRU 缓存实现
├── expire.go            # 过期键管理
├── aof.go               # AOF 持久化
├── rdb.go               # RDB 快照
├── replication.go       # 主从复制
└── consistency_hash.go  # 一致性哈希
```

## 快速开始

### 编译
```bash
go build -o miniredis .
```

### 启动主节点
```bash
./miniredis -port 9000 -aof=true
```

### 启动从节点
```bash
./miniredis -port 9001 -master=false -master-addr localhost:9000
```

### 命令行参数
| 参数 | 默认值 | 说明 |
|------|--------|------|
| -port | 9000 | TCP 服务端口 |
| -data | ./data | 数据持久化目录 |
| -node-id | node-1 | 集群节点 ID |
| -master | true | 是否为主节点 |
| -master-addr | localhost:9000 | 主节点地址 |
| -rdb-interval | 60 | RDB 快照间隔（秒） |
| -expire-interval | 10 | 过期键清理间隔（秒） |
| -aof | true | 是否启用 AOF |

## 支持的命令

| 命令 | 说明 | 示例 |
|------|------|------|
| GET | 获取值 | `GET key` |
| SET | 设置值 | `SET key value [PX ms]` |
| DEL | 删除值 | `DEL key` |
| PING | 心跳检测 | `PING` |
| INFO | 服务器信息 | `INFO` |
| TTL | 剩余时间 | `TTL key` |
| CLUSTER | 集群管理 | `CLUSTER NODES/ADD/REMOVE` |

## 使用示例

```bash
# 使用 redis-cli 连接
redis-cli -p 9000

# 设置值
SET mykey hello

# 获取值
GET mykey
"hello"

# 设置带过期时间的值（10秒）
SET tempkey value PX 10000

# 删除值
DEL mykey

# 查看服务器信息
INFO
```

## 持久化机制

### AOF（Append-Only File）
- 每次写操作追加到 `appendonly.aof`
- 支持智能重写压缩日志
- 默认启用

### RDB（Redis Database）
- 定时快照保存
- 默认每 60 秒保存一次

## 主从复制

1. 启动主节点
2. 启动从节点并指定主节点地址
3. 从节点自动同步主节点数据
4. 主节点写操作自动广播到从节点

## License

MIT License
