## API

对外提供GET/PUT/DELETE object的功能



## stream layer

提供Append Only的Stream, 只有partion layer使用, 对外提供AppendBlock和ReadBlock的功能

### build

```
make
```

1. 在子目录stream-manager调用goreman start启动stream manager(默认启动一个)
2. 在子目录extent-node调用goreman start启动nodes(默认启动3个)
3. 在子目录stream-client客户端测试工具: 运行```./stream-client wbench```压测性能

### 架构和IO流程

```
StreamManager管理stream, extent, nodes

ExtentNodes启动时在StreamManager上注册, 管理一台服务器上所有的extent

```

Append流程
```
Client从streamManager得到stream info, 知道最后一个extent的位置
Client发送AppendBlock到ExtentNode
ExtentNode采用Append的方式写3副本
Client得到返回的Offset

如果有超时错误或者client发现Offset超过2G, 调用StreamAllocExtent在当前stream分配一个新的extent,
写入新的extent
```


StreamAllocExtent流程:
```
1. streamManager首先调用commitLength到三副本, 选择commitLength最小值
2. 调用Seal(commitLength)到三副本, 不在乎返回值
3. streamManager选择出新的三副本,发送AllocExtent到nodes, 必须三个都成功才算成功
4. 修改结果写入ETCD
5. 修改sm里面的内存结构
```

目录结构

```
.
├── LICENSE
├── Makefile
├── README.md
├── cmd
│   ├── extent-node
│   │   ├── Makefile
│   │   ├── Procfile
│   │   ├── main.go
│   ├── stream-client
│   │   ├── Makefile
│   │   ├── main.go
│   └── stream-manager
│       ├── Makefile
│       ├── Procfile
│       ├── main.go
├── conn
│   ├── pool.go
│   └── snappy.go
├── extent
│   ├── extent.go
│   ├── extent.test
│   └── extent_test.go
├── manager
│   ├── config.go
│   ├── etcd.go
│   ├── etcd.log
│   ├── etcd_op.go
│   ├── etcd_op_test.go
│   ├── smclient
│   │   └── sm_client.go
│   └── streammanager
│       ├── policy.go
│       ├── sm.go
│       └── sm_service.go //grpc interface
├── node
│   ├── node.go
│   ├── node_service.go //grpc interface
│   ├── node_test.go
├── proto
│   ├── gen.sh
│   ├── pb.proto //stream layer
│   └── pspb.proto //partition layer
├── rangepartition
│   ├── commit_log.go
│   └── range_partion.go
├── streamclient
│   └── streamclient.go
├── utils
│   ├── lock.go
│   ├── stopper.go
│   ├── stopper_test.go
│   └── utils.go
└── xlog
    └── xlog.go
```

### extent结构


```
extent头(512字节)
	magic number (8字节)
	extent ID    (8字节)
block头 (512字节)
	checksum    (4字节)
	blocklength (4字节) //4k ~32M, 4k对齐
	userData     (小于等于512-8)
block数据
	数据  (4k对齐)

extent是否seal, 存储在文件系统的attr里面
"seal"=>"true"
```

主要API:
1. OpenExtent
2. AppendBlock (需要显式调用lock, 保证只有一个写发生)
3. ReadBlock
4. Seal

### extent node

管理extents,只知道本地的extent, 和extent对应的副本位置

对外API:

1. AppendBlock (自动复制3副本)
2. ReadBlock

内部集群API:
1. HeartBeat (node之间grpc conn保活)
2. ReplicateBlocks (primary node向secondary node复制副本)
3. AllocExtent  (创建extent, 由stream manager调用)
4. CommitLength (由stream manager调用)
5. Seal (由stream manager调用)

主要内存结构:
1. extendID => localFileName (在启动时打开所有extent的fd)

OpenExtent首先判断Extent是否是Sealed, 如果是Sealed的就正常打开.
如果不是Seal的, 说明Extent是正在被写入的, 打开时要检查Block的md5

#### extent node通信 

用```GetPool().Get(add)的场景``
1. node之间通信
2. stream manager连接node
3. client到node

#### 其他通信
1. client到stream manager
2. node到stream manager


### stream manager

实现采用embed etcd

通过本地127.0.0.1:XXX直接连接到ETCD
```
streamManager1 => etcd1
streamManager2 => etcd2
streamManager3 => etcd3
```

API:
```
	rpc StreamInfo(StreamInfoRequest) returns (StreamInfoResponse) {}
	rpc ExtentInfo(ExtentInfoRequest) returns (ExtentInfoResponse) {}
	rpc NodesInfo(NodesInfoRequest) returns(NodesInfoResponse) {}
	
	rpc StreamAllocExtent(StreamAllocExtentRequest) returns  (StreamAllocExtentResponse) {}
	rpc CreateStream(CreateStreamRequest) returns  (CreateStreamResponse) {}
	rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse) {}
```




ETCD存储结构:
```
streams/{id} => pb.StreamInfo
nodes/{id}   =>pb.NodeInfo
extents/{id} => pb.ExtentInfo
AutumnSmIDKey 存储已经分配的最大ID
AutumnSmLeader/xxx 存储当前leader的memberValue, 用来在leader写入时校验是否真的是leader
```

内存结构:

这些结构相当于etcd内容的cache.
```
streams    map[uint64]*pb.StreamInfo
extents   map[uint64]*pb.ExtentInfo
nodes    map[uint64]*NodeStatus
```

#### stream manager 选举

#####ETCD的transaction写入

```
	//在一个transaction写入不同的2个KEY
	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
		clientv3.OpPut(extentKey, string(edata)),
	}
	//在ops执行前, 做判断sm.leaderkey的值等于sm.memberValue
	err = manager.EtctSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)
```

#####选举相关

```
		//returns a new election on a given key prefix
		e := concurrency.NewElection(s, "session")
		ctx := context.TODO()

		if err = e.Campaign(ctx, "streamManager1"); err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		//存储leaderkey,之后所有的写入ETCD时, 都要验证leaderKey,保证是当前leader写入
		sm.leaderKey = e.Key()
		xlog.Logger.Infof("elected %d as leader", sm.ID)
		sm.runAsLeader()

		select {
		case <-s.Done():
			s.Close()
			atomic.StoreInt32(&sm.isLeader, 0)
			xlog.Logger.Info("%d's leadershipt expire", sm.ID)
		}
```


选举成功后:
1. 从etcd中读取数据到内存中
2. 把自己标志成leader

##### AllocID

所有的nodeID, extentID和streamID都是唯一的uint64, 由streamManager分配, 这个rpc可以由任何client调用,
不必要非要到leader


#### stream manager TODO

0. pb.Block可能需要增加offset选项, 保证写入都是幂等的, 这样可以在append block操作的时候, 如果有error, 可以先重试, 而不是直接申请新的extent
1. *实现node hearbteat, 和更精确的alloc policy*
2. *实现GC,检查extent的三副本是否完整和是否extent已经不被任何stream引用*
3. sm的实现中有3个函数很像: sendAllocToNodes, receiveCommitlength, sealExtents 不知道能不能统一
4. *实现Journal*
5. *实现EC*
6. stream manager client的代码可以简化
7. unit test全部缺少
8. 测试多ETCD的情况, 现在只测试了一个ETCD的情况
9. ETCD的key应该改成/clusterKey/node/0, /clusterKey/stream/1的情况, 防止多集群冲突
10. sm的内部数据结构能否改成https://github.com/hashicorp/go-memdb. 在不损失性能的情况下, 提高代码可读性
11. *node支持多硬盘*
12. 在sm里增加version, 每次nodes变化, version加1, 并且在rpc的返回里面增加version, 这样client根据version可以自动更新
13. 增加extent模块benchmark的内容(mac SSD上面, sync 4k需要30ms?!!), 现在benchmark的结果只有4k
14. extent也有很大的优化空间, AppendBlock发到每块硬盘的队列上, 然后取队列, 写数据, 再sync,可以减少单块硬盘上的sync次数. 但是: 如果有SSD
journal的话, 这些优化可能都不需要
15. streamclient增加自动Seal的功能
16. extent层用mmap,提升读性能

## partion layer

ETCD存储结构in PM(Partition Manager)

```
PART_%d/range => [startKey, endKey]
PART_%d/blobStreams => [id,...,id]
PART_%d/logStream => id
PART_%d/rowStream => id
PART_%d/tables => [(extentID,offset),...,(extentID,offset)]
```

1.增加Option便于测试
2.grpc里面, res.Code代替err
3.mockclient如何测试truncate
4.sm_service增加truncate的API
5.rp实现valuelog的truncate(*)