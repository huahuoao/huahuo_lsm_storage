package main

import (
	"github.com/huahuoao/lsm-core/internal/etcd"
	"github.com/huahuoao/lsm-core/internal/protocol"
	"github.com/huahuoao/lsm-core/internal/storage"
	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
	"log"
	"time"
)

var Hbase *storage.Hbase

func NewTCPPool() {
	ss := protocol.NewBluebellServer("tcp", "0.0.0.0:9000", true)
	options := []gnet.Option{
		gnet.WithMulticore(true),               // 启用多核模式
		gnet.WithReusePort(true),               // 启用端口重用
		gnet.WithTCPKeepAlive(time.Minute * 5), // 启用 TCP keep-alive
		gnet.WithReadBufferCap(2048 * 1024),
		gnet.WithWriteBufferCap(2048 * 1024),
	}
	err := gnet.Run(ss, ss.Network+"://"+ss.Addr, options...)
	logging.Infof("node exits with error: %v", err)
}

func main() {
	go NewTCPPool()
	var err error
	Hbase, err = storage.NewHbaseClient()
	if err != nil {
		panic(err)
	}
	endpoints := []string{"localhost:2379"}
	rc, err := etcd.NewRegistryClient(endpoints)
	if err != nil {
		log.Fatalf("Failed to create registry client: %v", err)
	}
	_ = rc.Register("localhost:9000")
	select {}
}
