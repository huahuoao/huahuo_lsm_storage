package main

import (
	"github.com/huahuoao/lsm-core/internal/protocol"
	"github.com/huahuoao/lsm-core/internal/storage"
	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
	"sync"
	"time"
)

var Hbase *storage.Hbase

func NewTCPPool(wg *sync.WaitGroup) {
	defer wg.Done()
	ss := protocol.NewBluebellServer("tcp", "0.0.0.0:9000", true)
	options := []gnet.Option{
		gnet.WithMulticore(true),               // 启用多核模式
		gnet.WithReusePort(true),               // 启用端口重用
		gnet.WithTCPKeepAlive(time.Minute * 5), // 启用 TCP keep-alive
		gnet.WithReadBufferCap(2048 * 1024),
		gnet.WithWriteBufferCap(2048 * 1024),
	}
	err := gnet.Run(ss, ss.Network+"://"+ss.Addr, options...)
	logging.Infof("server exits with error: %v", err)
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go NewTCPPool(&wg)
	var err error
	Hbase, err = storage.NewHbaseClient()
	if err != nil {
		panic(err)
	}
	wg.Wait()
}
