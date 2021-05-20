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



### extent结构


```

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
11. *node支持多硬盘*
12. *在sm里增加version, 每次nodes变化, version加1, 并且在rpc的返回里面增加version, 这样client根据version可以自动更新*, ref count
13. extent层用mmap,提升读性能

## partion layer

ETCD存储结构in PM(Partition Manager)

```

PART/{PartID}/logStream => id <MUST>
PART/{PartID}/rowStream => id <MUST>
PART/{PartID}/parent = PSID <MUST>
PART/{PartID}/range = <startKey, endKey> <MUST>
PART/{PartID}/tables => [(extentID,offset),...,(extentID,offset)]
PART/{PartID}/blobStreams => [id,...,id]
PART/{PartID}/discard => <DATA>

PSSERVER/{PSID} => {PSDETAIL}
//when updating PART/*/range. update PSVERSION
PSVERSION  => {num}
```




## TODO
2. rp实现valuelog的truncate(*)
3. 实现logstream分为2个不同的stream,一个可以在生成memtable后直接删除, 另一个长久保存(定期recycle或者EC化)
4. ps merge / split

### LOG

```
+---------+-----------+-----------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Payload   |
+---------+-----------+-----------+--- ... ---+
```



### sm service
(a) maintaining the stream namespace and state of all active streams and extents DONE
(b) monitoring the health of the ENs                                             DONE 
+ node api: usage                                                                DONE
+ sm : (go func)update usage and set dead flag if node is not responding in n minute
(c) creating and assigning extents to ENs                                        DONE
(d) performing the lazy re-replication of extent replicas that are lost due to hardware failures or unavailability DONE
+ (go func)loop over extentInfo, if it has dead node, replicas the data(one by one) DONE
(e) garbage collecting extents that are no longer pointed to by any stream ==> (ref count) 
+ node: (go func)loop over extents, ask extentInfo, if (ref count) == 0 , delete file TODO

(f) disk failure,
f1. local copy(keep extentInfo the same, faster)   TODO
f2. send "remove extent from node" to manager, manager schedule tasks(one by one) TODO




ETCD存储结构

1. 可管理
2. 处理recovery Node失败的情况
3. 同一个extent,只有一个进程在恢复


1. 数据恢复

准备一个SET, 持久化在etcd里, 里面/recoveryTasks是需要执行的的recovery任务,
只有在成功恢复了一个extent之后, 才在etcd里面删除这个task, 同时在leader的内存里面
存储taskPool, 这个和SET保持一致

/recoveryTasks/extentID.tsk

这个SET有2个来源
1. stream manager scan得到()
2. 由node主动提出, 比如node有一个disk failure,
   2.a 如果是全盘raid5, raid5自行处理(目前选择)
   2.b node自己处理, 把extent转移到其他硬盘, 这种情况不需要修改元数据
   2.c node自己处理, 其他硬盘容量不够, node提一个api, 请上层处理

/recoveryTaskLocks/extentID.lck


stream manager运行, 主要功能是向SET里面塞任务
routineFixReplics(每分钟一次)
	-> 如果存在extent的node是dead状态
	   -> queueRecoveryTask
	   1. dlock(如果失败, 说明有其他node正在进行recovery, 直接返回)
	   2. 再读extentInfo一遍, 确认
	   3. 提交recovery_task到SET(如果之前有任务, 提交失败)
	   4. dlock.unlock

其中2跟一个race condition相关, 主要好处是避免过多的dlock


routineDispatchTask(每分钟一次)
	-> 读taskPool
	   1. 如果说有recovery在进行, 但是启动时间很久之前, 分配任务
	   2. 如果说recovery没有在进行, 分配任务
	   这2个都有可能是重复任务, 依赖在node中的dlock保证唯一

	   3. 如果分配任务返回OK, 说明远程正在执行恢复任务, 然后更新taskPool


Node:

RequireRecovery
1. Dlock
2. 如果自己的load太高, 拒绝, 然后unlock
3. runRecoveryTask,返回已经接收任务
    -> runRecoveryTask  拷贝一个副本到本node
	a. 返回时Dlock.unlock
	b. EC和Replicat的恢复有不同, 但是都返回一个在diskFS里面的File

    c. 新文件加入到node管理的extent中
	d. 返回task成功, CopyExtentDone提示manager更新元数据

stream manager的逻辑
copyExtentDone
1. 检查dlock确实被别人锁住
2. 更新extentInfo, version++
3. 删除SET中的任务
4. 更新taskPool




重复提交task

1.==========如果"创建task"在"更新extentInfo(内存)"之前运行

manager                    manager_service(from node)


创建task(如果有dead node)
                    	 更新extentInfo (etcd)
                	     删除etcd task    (etcd)
						 更新extentInfo(内存)/删除recovies
create task(etcd)       



目前rangepartiion, gc的bug
GC           USER
read V
             write V1000
			 MEMTABLE flushed
write V45
除了seqNum, 还有增加rotateNum, 
目前认为memtable有序, 之后的所有读, 只能读出V45
