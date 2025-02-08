package main

import (
	"fmt"
	"github.com/huahuoao/lsm-core/internal/storage/engine/lsmtree"
)

func main() {
	fmt.Println(lsmtree.GetDatabaseSourcePath())
	fmt.Println("hello world2")
}
