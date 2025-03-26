package etcd

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

type RegistryClient struct {
	client *clientv3.Client
	lease  clientv3.Lease
}

func NewRegistryClient(endpoints []string) (*RegistryClient, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &RegistryClient{
		client: client,
		lease:  clientv3.NewLease(client),
	}, nil
}

func (rc *RegistryClient) Register(ip string) error {
	ctx := context.Background()

	// 创建租约
	leaseResp, err := rc.lease.Grant(ctx, 5)
	if err != nil {
		return err
	}

	// 存储字符串
	key := fmt.Sprintf("/registry/ips/%s", ip)
	_, err = rc.client.Put(ctx, key, time.Now().String(), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return err
	}
	// 启动自动续约
	go rc.keepAlive(leaseResp.ID)

	return nil
}

func (rc *RegistryClient) keepAlive(leaseID clientv3.LeaseID) {
	ctx := context.Background()
	ch, err := rc.lease.KeepAlive(ctx, leaseID)
	if err != nil {
		log.Printf("Failed to keep alive lease %d: %v", leaseID, err)
		return
	}

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Printf("Keep alive channel closed for lease %d", leaseID)
				return
			}
		}
	}
}
