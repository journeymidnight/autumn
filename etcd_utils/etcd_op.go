package etcd_utils

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/coreos/etcd/mvcc/mvccpb"
)




func EtcdWatchEvents(c *clientv3.Client, start, end string, startRev int64) (clientv3.WatchChan, func()) {
	watcher := clientv3.NewWatcher(c)

	var rch clientv3.WatchChan
	if len(end) > 0 {
		rch = watcher.Watch(context.Background(), start, 
		clientv3.WithRange(end), 
		clientv3.WithPrevKV(),
		clientv3.WithRev(startRev))
	} else {
		rch = watcher.Watch(context.Background(), start, 
		clientv3.WithPrevKV(),
		clientv3.WithRev(startRev))

	}

	return rch, func(){
		watcher.Close()
	}
}

func EtcdSetKV(c *clientv3.Client, key string, val []byte, opts ...clientv3.OpOption) error {
	kv := clientv3.NewKV(c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := kv.Put(ctx, key, string(val), opts...)
	return err
}

func EtcdSetKVS(c *clientv3.Client, cmps []clientv3.Cmp, ops []clientv3.Op) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn := clientv3.NewKV(c).Txn(ctx)
	txn.If(cmps...)
	res, err := txn.Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return errors.New("set etcd kv failed, maybe mutex not match")
	}
	return nil
}

func EtcdGetKV(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		return nil, 0,  err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return nil, 0, nil
	}
	
	return resp.Kvs[0].Value,resp.Header.Revision, nil
}

//EtcdRange return (KeyValue, Revision, error)
func EtcdRange(c *clientv3.Client, prefix string) ([]*mvccpb.KeyValue, int64, error) {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(0),
	}
	kv := clientv3.NewKV(c)
	ctx := context.Background()
	resp, err := kv.Get(ctx, prefix, opts...)
	
	if err != nil {
		return nil, 0, err
	}
	return resp.Kvs, resp.Header.Revision, err
}

func EtcdAllocUniqID(c *clientv3.Client, idKey string, count uint64) (uint64, uint64, error) {

	curValue, _, err := EtcdGetKV(c, idKey)
	if err != nil {
		return 0, 0, err
	}

	//build txn, compare and set ID
	var cmp clientv3.Cmp
	var curr uint64 = 1

	if curValue == nil {
		cmp = clientv3.Compare(clientv3.CreateRevision(idKey), "=", 0)
	} else {
		curr = binary.BigEndian.Uint64(curValue)
		cmp = clientv3.Compare(clientv3.Value(idKey), "=", string(curValue))
	}

	var newValue [8]byte
	binary.BigEndian.PutUint64(newValue[:], curr+count)

	txn := clientv3.NewKV(c).Txn(context.Background())
	t := txn.If(cmp)
	_, err = t.Then(clientv3.OpPut(idKey, string(newValue[:]))).Commit()
	if err != nil {
		return 0, 0, err
	}
	return curr, curr + count, nil

}
