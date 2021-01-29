package manager

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pkg/errors"
)

/*
var (
	versionKey = "AspiraVersionKey"
)
*/

func EtcdSetKV(c *clientv3.Client, key string, val []byte, opts ...clientv3.OpOption) error {
	kv := clientv3.NewKV(c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := kv.Put(ctx, key, string(val), opts...)
	return err
}

func EtctSetKVS(c *clientv3.Client, cmps []clientv3.Cmp, ops []clientv3.Op) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn := clientv3.NewKV(c).Txn(ctx)
	txn.If(cmps...)
	res, err := txn.Then(ops...).Commit()
	if res.Succeeded == false || err != nil {
		return errors.Wrap(err, "SetKVs failed")
	}
	return nil
}

func EtcdGetKV(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func EtcdRange(c *clientv3.Client, prefix string) ([]*mvccpb.KeyValue, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(0),
	}
	kv := clientv3.NewKV(c)
	ctx := context.Background()
	resp, err := kv.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, err
	}
	return resp.Kvs, err
}
