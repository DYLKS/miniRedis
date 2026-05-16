package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// RDB 文件格式常量
const (
	rdbMagic   = "REDIS" // RDB 文件标识
	rdbVersion = 0x05    // RDB 版本号
)

// SaveRDB 将缓存数据保存为 RDB 格式文件
// RDB 格式：magic(5) + version(1) + metadata + data + checksum
func SaveRDB(cache *LRUCache, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	filePath := filepath.Join(dataDir, "dump.rdb")
	tempPath := filePath + ".tmp"
	f, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	// 写入 MAGIC 和 VERSION
	writer.WriteString(rdbMagic)
	binary.Write(writer, binary.LittleEndian, uint8(rdbVersion))

	// 写入元数据
	now := uint64(time.Now().Unix())
	writer.WriteByte(0xfa)
	writeLen(writer, []byte("redis-ver"))
	writeLen(writer, []byte("6.0.0"))

	writer.WriteByte(0xfa)
	writeLen(writer, []byte("redis-bits"))
	binary.Write(writer, binary.LittleEndian, uint64(64))

	writer.WriteByte(0xfa)
	writeLen(writer, []byte("ctime"))
	binary.Write(writer, binary.LittleEndian, now)

	// 写入数据
	data := cache.GetAll()

	writer.WriteByte(0xfb)
	binary.Write(writer, binary.LittleEndian, uint64(len(data)))

	for key, value := range data {
		writer.WriteByte(0) // type = string
		writeLen(writer, []byte(key))
		writeLen(writer, []byte(value))
	}

	// 写入校验和（当前为占位实现）
	writer.WriteByte(0xff)
	checksum := crc64Checksum(writer)
	binary.Write(writer, binary.LittleEndian, checksum)

	writer.Flush()

	// 原子性替换原文件
	return os.Rename(tempPath, filePath)
}

// LoadRDB 从 RDB 文件加载数据到缓存
// 解析 RDB 格式并恢复数据
func LoadRDB(cache *LRUCache, dataDir string) error {
	filePath := filepath.Join(dataDir, "dump.rdb")
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在不算错误
		}
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	// 读取并验证 MAGIC
	magic := make([]byte, 5)
	if _, err := reader.Read(magic); err != nil {
		return err
	}
	if string(magic) != rdbMagic {
		return fmt.Errorf("invalid RDB magic")
	}

	// 读取版本号
	var version uint8
	if err := binary.Read(reader, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != rdbVersion {
		return fmt.Errorf("unsupported RDB version")
	}

	// 解析内容
	for {
		byteVal, err := reader.ReadByte()
		if err != nil {
			break
		}

		switch byteVal {
		case 0xfa:
			// 元数据键值对
			readLen(reader)
			readLen(reader)
		case 0xfb:
			// 数据部分
			var count uint64
			binary.Read(reader, binary.LittleEndian, &count)
			for i := uint64(0); i < count; i++ {
				reader.ReadByte() // type
				key := readLen(reader)
				value := readLen(reader)
				cache.Set(string(key), string(value))
			}
		case 0xff:
			// EOF 标记
			return nil
		default:
			return fmt.Errorf("unknown RDB opcode: %x", byteVal)
		}
	}
	return nil
}

// writeLen 使用变长编码写入数据长度和内容
// 编码规则：
//
//	< 0x80: 单字节长度
//	< 0x4000: 两字节长度（高位标记）
//	< 0x20000000: 三字节长度 + 4字节数据
//	其他: 四字节长度 + 8字节数据
func writeLen(writer *bufio.Writer, data []byte) {
	length := len(data)
	if length < 0x80 {
		writer.WriteByte(uint8(length))
	} else if length < 0x4000 {
		writer.WriteByte(uint8(length>>8) | 0x80)
		writer.WriteByte(uint8(length & 0xff))
	} else if length < 0x20000000 {
		writer.WriteByte(0xc0)
		binary.Write(writer, binary.LittleEndian, uint32(length))
	} else {
		writer.WriteByte(0xd0)
		binary.Write(writer, binary.LittleEndian, uint64(length))
	}
	writer.Write(data)
}

// readLen 读取变长编码的长度
func readLen(reader *bufio.Reader) []byte {
	firstByte, _ := reader.ReadByte()
	var length uint64

	switch firstByte & 0xf0 {
	case 0x00:
		length = uint64(firstByte)
	case 0x80:
		secondByte, _ := reader.ReadByte()
		length = uint64(firstByte&0x7f)<<8 | uint64(secondByte)
	case 0xc0:
		binary.Read(reader, binary.LittleEndian, &length)
	case 0xd0:
		binary.Read(reader, binary.LittleEndian, &length)
	default:
		return nil
	}

	data := make([]byte, length)
	reader.Read(data)
	return data
}

// crc64Checksum 计算 RDB 文件校验和（占位实现）
func crc64Checksum(writer *bufio.Writer) uint64 {
	return 0
}

// StartRDBLoop 启动定时 RDB 快照保存协程
// 定期将缓存数据保存到 RDB 文件
func StartRDBLoop(cache *LRUCache, dataDir string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		SaveRDB(cache, dataDir)
	}
}
