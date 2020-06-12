package my_api

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var schema = "bv-service-resolver"

type ServiceInfo struct {
	Name string
	Host string
	Port int
}

// Register
func Register(target string, info *ServiceInfo, ttl int) error {
	serviceName := info.Name
	serviceHostPort := net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
	serviceKey := fmt.Sprintf("/%s/%s/%s", schema, serviceName, serviceHostPort)

	// get endpoints for register dial address
	var err error
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(target, ";"),
	})
	if err != nil {
		return fmt.Errorf("grpclb: create clientv3 client failed: %v", err)
	}

	// 创建租约
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	resp, err := cli.Grant(ctx, int64(ttl))
	if err != nil {
		return fmt.Errorf("grpclb: create clientv3 lease failed: %v", err)
	}

	// 添加Key Value
	bytesJson, _ := json.Marshal(info)
	ctx, cancel = context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	if _, err := cli.Put(ctx, serviceKey, string(bytesJson), clientv3.WithLease(resp.ID)); err != nil {
		return fmt.Errorf("grpclb: set service '%s' with ttl to clientv3 failed: %s", serviceName, err.Error())
	}

	// 保活, 内部会产生go routine, 一直保活
	kpChan, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		return fmt.Errorf("grpclb: refresh service '%s' with ttl to clientv3 failed: %s", serviceName, err.Error())
	}

	go func() {
		// 如果中间出现保活失败, 调用者如何获得
		// 考虑添加参数context, 用户不取消, 则保活失败后, 尝试保活、尝试注册保活
		for {
			select {
			case rs, ok := <-kpChan:
				_ = ok
				if rs == nil {
					log.Print("租约已经到期关闭")
					return
				} else {
					// 续租成功
					log.Printf("续租成功 %v", rs.TTL)
				}
			}
		}
	}()

	return nil
}

// UnRegister delete registered service from etcd
func UnRegister(target string, info *ServiceInfo) error {
	serviceName := info.Name
	serviceHostPort := net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
	serviceKey := fmt.Sprintf("/%s/%s/%s", schema, serviceName, serviceHostPort)

	// get endpoints for register dial address
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(target, ";"),
	})
	if err != nil {
		return fmt.Errorf("grpclb: create clientv3 client failed: %v", err)
	}

	// 删除租约
	{
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()
		getRes, err := cli.Get(ctx, serviceKey)
		if err != nil {
			// 没有该key
			return err
		}

		// 删除key对应的租约(该项待议)
		for _, kv := range getRes.Kvs {
			_, err = cli.Revoke(context.Background(), clientv3.LeaseID(kv.Lease))
			log.Printf("revokey lease [%v], key: [%v] error: [%v]", kv.Lease, string(kv.Key), err)
		}
	}

	// 删除key
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	_, err = cli.Delete(ctx, serviceKey)
	if err != nil {
		return err
	} else {
		log.Printf("delelte key [%v] success", serviceKey)
	}

	return nil
}
