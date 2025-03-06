package dispatcher

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type RegistryClient struct {
	client *clientv3.Client
	lease  clientv3.Lease
}

func NewRegistryClient(endpoints []string) (*RegistryClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &RegistryClient{
		client: cli,
		lease:  clientv3.NewLease(cli),
	}, nil
}

// AddIP 注册IP地址（带租约自动清理）
func (rc *RegistryClient) AddIP(ip string) error {
	ctx := context.Background()

	// 创建租约（15秒TTL）
	leaseResp, err := rc.lease.Grant(ctx, 5)
	if err != nil {
		return err
	}

	// 存储IP地址
	key := fmt.Sprintf("/registry/ips/%s", ip)
	_, err = rc.client.Put(ctx, key, time.Now().String(), clientv3.WithLease(leaseResp.ID))
	return err
}

// QueryIPs 查询所有已注册IP地址
func (rc *RegistryClient) QueryIPs() ([]string, error) {
	ctx := context.Background()
	resp, err := rc.client.Get(ctx, "/registry/ips/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	ips := make([]string, 0)
	for _, kv := range resp.Kvs {
		ips = append(ips, string(kv.Key[len("/registry/ips/"):]))
	}
	return ips, nil
}

// WatchIPChanges 监听IP注册变化
func (rc *RegistryClient) WatchIPChanges() {
	watcher := clientv3.NewWatcher(rc.client)
	watchChan := watcher.Watch(context.Background(), "/registry/ips/", clientv3.WithPrefix())

	for resp := range watchChan {
		for _, ev := range resp.Events {
			ip := string(ev.Kv.Key[len("/registry/ips/"):])
			switch ev.Type {
			case clientv3.EventTypePut:
				fmt.Printf("[INFO] IP added: %s (Revision: %d)\n", ip, ev.Kv.CreateRevision)
				GetRing().Add(ip)
			case clientv3.EventTypeDelete:
				fmt.Printf("[WARN] IP expired/deleted: %s (Revision: %d)\n", ip, ev.Kv.ModRevision)
				GetRing().Remove(ip)
			}
		}
	}
}
