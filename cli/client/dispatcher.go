package client

import (
	"fmt"
	"strconv"
	"strings"
)

var RegistryCli *RegistryClient

func GetRegistryCli() *RegistryClient {
	if RegistryCli == nil {
		fmt.Println("cli is nil")
		return nil
	}

	return RegistryCli
}

func DispatcherInit(addr string) {
	cli, err := NewRegistryClient([]string{addr})
	if err != nil {
		panic(err)
	}
	ips, _ := cli.QueryIPs()
	fmt.Println(ips)
	for _, ip := range ips {
		parts := strings.Split(ip, ":")
		addr := parts[0]
		port, _ := strconv.Atoi(parts[1])
		HuaHuoLsmCli.Clients[ip] = New(addr, port)
		HuaHuoLsmCli.Clients[ip].Start()
		HuaHuoLsmCli.Clients[ip].Status = true
		GetRing().Add(ip)
	}
	// 启动监听协程
	go cli.WatchIPChanges()
}
