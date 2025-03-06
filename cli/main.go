package main

import (
	"github.com/huahuoao/lsm-core/client/dispatcher"
	"time"
)

func main() {
	dispatcher.Init("localhost:2379")
	time.Sleep(5 * time.Second)

	select {}
}
