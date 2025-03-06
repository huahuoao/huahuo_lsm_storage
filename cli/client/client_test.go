package client

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"math/rand"
)

type MyStruct struct {
	Name string
	Age  int
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func singleClient(wg *sync.WaitGroup) {
	defer wg.Done()
	c := New("localhost", 9000, "localhost:2379")
	c.Start()
	startTime := time.Now() // 记录开始时间

	for i := 0; i < 100; i++ {
		key := randomString(1024)
		c.SetString(key, randomString(1024))
		c.GetString(key)

	}
	duration := time.Since(startTime) // 计算持续时间

	fmt.Printf("singleClient took %v\n", duration) // 输出时长

}

// 测试并发
func TestConcurrent(t *testing.T) {
	startTime := time.Now() // 记录开始时间
	var wg sync.WaitGroup
	count := 1
	wg.Add(count)
	for i := 0; i < count; i++ {
		go singleClient(&wg)
	}
	wg.Wait()
	duration := time.Since(startTime)          // 计算持续时间
	t.Logf("TestConcurrent took %v", duration) // 输出时长
}

// func Test(t *testing.T) {
// 	c := New("localhost", 9000)
// 	err := c.Start()
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	c.NewGroup("huahuo", 128*MB)
// 	c.BindGroup("huahuo")
// 	random := randomString(1024)
// 	c.SetString("test_string", random)
// 	s, err := c.GetString("test_string")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	c.SetStruct("test", &MyStruct{Name: "huahuo", Age: 20})
// 	var obj MyStruct
// 	err = c.GetStruct("test", &obj)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if s != random || obj.Name != "huahuo" || obj.Age != 20 {
// 		t.Error("test failed")
// 	}
// 	c.Del("test_string")
// 	c.NewGroup("huahuo1", 128*MB)
// 	c.DelGroup("huahuo1")
// }
