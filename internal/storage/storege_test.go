package storage

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// RandStringBytesGenerate 生成指定长度的随机字符串
func RandStringBytesGenerate(n int) string {
	var result string
	for i := 0; i < n; i++ {
		result += string(charset[rand.Intn(len(charset))])
	}
	return result
}
func TestStorage(t *testing.T) {
	count := 8000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	h, err := NewHbaseClient()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now() // 记录开始时间
	for i := 0; i < count; i++ {
		v := RandStringBytesGenerate(1024)
		h.Put([]byte("key"+string(rune(i))), []byte(v))
		keys[i] = []byte("key" + string(rune(i)))
		values[i] = []byte(v)
	}
	elapsed := time.Since(start) // 计算执行时间
	t.Logf("存储 %d 个键值对耗时: %s", count, elapsed)
	// 验证存储的键值对
	for i := 0; i < count; i++ {
		val, exist := h.Get(keys[i])
		if !exist {
			t.Errorf("获取键值对时出错: %v", err)
		}
		if !bytes.Equal(val, values[i]) {
			t.Errorf("键 %s 对应的值不匹配，期望 %v，实际 %v", keys[i], values[i], val)
		}
	}
	h.tree.PrintStatus()
}
