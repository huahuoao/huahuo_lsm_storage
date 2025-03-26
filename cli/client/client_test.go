package client

import (
	"fmt"
	"math/rand"
	"testing"
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

func Test1(t *testing.T) {
	////这边有nil panic 待排查
	LsmCliInit()
	DispatcherInit("localhost:2379")
	err := HuaHuoLsmCli.Set("测试key", []byte("测试value"))
	value, err := HuaHuoLsmCli.Get("测试key")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("get value: %v \n\n", string(value))
	fmt.Println("测试结束")
	select {}
}
