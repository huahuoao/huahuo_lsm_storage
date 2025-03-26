package main

import (
	"github.com/huahuoao/lsm-core/client/client"
)

func main() {
	client.DispatcherInit("localhost:2379")
	client.LsmCliInit()

	select {}
}
