package storage

import (
	"testing"
	"time"
)

func TestStorage(t *testing.T) {
	h, err := NewHbaseClient()
	if err != nil {
		t.Fatal(err)
	}

	// 测试不同数量级的数据存储
	for _, count := range []int{10, 100, 1000} {
		start := time.Now() // 记录开始时间

		for i := 0; i < count; i++ {
			h.Put([]byte("key1"+string(rune(i))), []byte("value1"))
		}

		value, exist := h.Get([]byte("key11"))
		if !exist {
			t.Fatal("key not exist")
		}
		t.Log(string(value))

		elapsed := time.Since(start) // 计算执行时间
		t.Logf("存储 %d 个键值对耗时: %s", count, elapsed)
	}
}
