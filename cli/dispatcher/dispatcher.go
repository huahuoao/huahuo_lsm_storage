package dispatcher

func Init(addr string) {
	client, err := NewRegistryClient([]string{addr})
	if err != nil {
		panic(err)
	}
	// 启动监听协程
	go client.WatchIPChanges()
}
