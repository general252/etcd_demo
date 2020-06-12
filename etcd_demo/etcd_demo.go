package main

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

// 使能授权
func enableAuth(cli *clientv3.Client) error {
	if _, err := cli.UserAdd(context.Background(), "root", "123"); err != nil {
		log.Print(err)
		return err
	}

	if _, err := cli.AuthEnable(context.Background()); err != nil {
		log.Print(err)
		return err
	}

	return nil
}

// 监听键变化
func watchKey(cli *clientv3.Client) {
	watchRes := cli.Watch(context.Background(), "", clientv3.WithPrefix())
	for watch := range watchRes {
		for _, evt := range watch.Events {
			log.Printf("%s %q : %q", evt.Type, evt.Kv.Key, evt.Kv.Value)
		}
	}
}

// 遍历所有键值对
func listAllKey(cli *clientv3.Client) {

	for {
		time.Sleep(time.Second)
		getRes, err := cli.Get(context.Background(), "", clientv3.WithPrefix())
		if err != nil {
			log.Print(err)
			return
		}

		for _, k := range getRes.Kvs {
			var ttl int64 = 0
			if k.Lease > 0 {
				leaseInfo, err := cli.TimeToLive(context.Background(), clientv3.LeaseID(k.Lease))
				if err != nil {
					log.Print(err)
				} else {
					ttl = leaseInfo.TTL
				}
			}

			log.Printf("key: %v lease: %v, ttl: %v", string(k.Key), k.Lease, ttl)
		}
	}
}

func main() {
	var endpoints = []string{"localhost:2379", "localhost:22379", "localhost:32379"}
	var dialTimeout = 5 * time.Second
	// now check the permission with the root account
	rootCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		//Username:    "root",
		//Password:    "123",
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer rootCli.Close()

	go watchKey(rootCli)
	go listAllKey(rootCli)

	time.Sleep(time.Hour)
}
