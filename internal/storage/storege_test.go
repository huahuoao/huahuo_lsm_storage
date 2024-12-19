package storage

import (
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
	h, err := NewHbaseClient()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now() // 记录开始时间
	count := 500
	for i := 0; i < count; i++ {
		h.Put([]byte("key"+string(rune(i))), []byte(RandStringBytesGenerate(1024)))
	}
	elapsed := time.Since(start) // 计算执行时间
	t.Logf("存储 %d 个键值对耗时: %s", count, elapsed)

	h.tree.PrintStatus()
}
