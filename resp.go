package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// RESPType RESP 协议数据类型标识
// Redis Serialization Protocol 类型字节
type RESPType byte

// RESP 协议五种基本类型常量
const (
	RESPString  RESPType = '+' // 简单字符串：+OK\r\n
	RESPError   RESPType = '-' // 错误类型：-ERR unknown command\r\n
	RESPInteger RESPType = ':' // 整数类型：:1000\r\n
	RESPBulk    RESPType = '$' // 批量字符串：$5\r\nhello\r\n
	RESPArray   RESPType = '*' // 数组类型：*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
)

// RESPValue RESP 协议值结构体
// 支持五种数据类型：字符串、错误、整数、批量字符串、数组
type RESPValue struct {
	Type  RESPType    // 数据类型
	Str   string      // 字符串值（用于 Simple String 和 Error）
	Int   int64       // 整数值
	Bulk  []byte      // 批量字符串值
	Array []RESPValue // 数组值
}

// String 将 RESPValue 序列化为 RESP 协议格式字符串
func (v RESPValue) String() string {
	switch v.Type {
	case RESPString:
		return fmt.Sprintf("+%s\r\n", v.Str)
	case RESPError:
		return fmt.Sprintf("-%s\r\n", v.Str)
	case RESPInteger:
		return fmt.Sprintf(":%d\r\n", v.Int)
	case RESPBulk:
		if v.Bulk == nil {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(v.Bulk), v.Bulk)
	case RESPArray:
		if v.Array == nil {
			return "*-1\r\n"
		}
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("*%d\r\n", len(v.Array)))
		for _, item := range v.Array {
			buf.WriteString(item.String())
		}
		return buf.String()
	default:
		return ""
	}
}

// RESPParser RESP 协议解析器
type RESPParser struct {
	reader *bufio.Reader
}

func NewRESPParser(r io.Reader) *RESPParser {
	return &RESPParser{
		reader: bufio.NewReader(r),
	}
}

func (p *RESPParser) ReadLine() ([]byte, error) {
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r")), nil
}

// Parse 解析完整的 RESP 值
func (p *RESPParser) Parse() (RESPValue, error) {
	for {
		line, err := p.ReadLine()
		if err != nil {
			return RESPValue{}, err
		}
		if len(line) == 0 {
			continue // 跳过空行
		}

		respType := RESPType(line[0])
		switch respType {
		case RESPString, RESPError:
			return RESPValue{Type: respType, Str: string(line[1:])}, nil
		case RESPInteger:
			val, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				return RESPValue{}, err
			}
			return RESPValue{Type: RESPInteger, Int: val}, nil
		case RESPBulk:
			length, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				return RESPValue{}, err
			}
			if length == -1 {
				return RESPValue{Type: RESPBulk, Bulk: nil}, nil
			}
			bulk := make([]byte, length+2)
			_, err = io.ReadFull(p.reader, bulk)
			if err != nil {
				return RESPValue{}, err
			}
			return RESPValue{Type: RESPBulk, Bulk: bulk[:length]}, nil
		case RESPArray:
			length, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				return RESPValue{}, err
			}
			if length == -1 {
				return RESPValue{Type: RESPArray, Array: nil}, nil
			}
			array := make([]RESPValue, length)
			for i := int64(0); i < length; i++ {
				val, err := p.Parse()
				if err != nil {
					return RESPValue{}, err
				}
				array[i] = val
			}
			return RESPValue{Type: RESPArray, Array: array}, nil
		default:
			return RESPValue{}, fmt.Errorf("unknown type: %c", respType)
		}
	}
}

// ===== RESP 编码函数 =====

func EncodeString(s string) RESPValue {
	return RESPValue{Type: RESPString, Str: s}
}

func EncodeError(s string) RESPValue {
	return RESPValue{Type: RESPError, Str: s}
}

func EncodeInteger(i int64) RESPValue {
	return RESPValue{Type: RESPInteger, Int: i}
}

func EncodeBulk(b []byte) RESPValue {
	return RESPValue{Type: RESPBulk, Bulk: b}
}

func EncodeArray(arr []RESPValue) RESPValue {
	return RESPValue{Type: RESPArray, Array: arr}
}
