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

### stream manager

实现采用embed etcd

通过本地127.0.0.1:XXX直接连接到ETCD
```
streamManager1 => etcd1
streamManager2 => etcd2
streamManager3 => etcd3
```


ETCD存储结构:
```
streams/{id} => pb.StreamInfo
nodes/{id}   => pb.NodeInfo
disks/{id}   => pb.DiskInfo
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
	err = etcd_utils.EtctSetKVS(sm.client, []clientv3.Cmp{
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
1. *更精确的alloc policy*
2. *实现GC,检查extent的三副本是否完整和是否extent已经不被任何stream引用*
3. sm的实现中有3个函数很像: sendAllocToNodes, receiveCommitlength, sealExtents 不知道能不能统一
8. 测试多ETCD的情况, 现在只测试了一个ETCD的情况
9. ETCD的key应该改成/clusterKey/node/0, /clusterKey/stream/1的情况, 防止多集群冲突
13. extent层用mmap,提升读性能

## partion layer

ETCD存储结构in PM(Partition Manager)

```

PART/{PartID} => {id, id <startKey, endKEY>} //immutable

PARTSTATS/{PartID}/tables => [(extentID,offset),...,(extentID,offset)]
PARTSTATS/{PartID}/blobStreams => map[id,...,id]
PARTSTATS/{PartID}/discard => <DATA>


PSSERVER/{PSID} => {PSDETAIL}

修改为Partition到PS的映射
regions/config => {
	{part1,: ps3, region}, {part4: ps5, region}
}

//lock service
LOCKS/{PARTID}

```

1. PS启动后watch Distribute/config, 如果有分到自己的pg, 载入, 如果自己的pg分走, unload
2. Distribute由pm计算
    2.a 如果PSSERVER变化, 重新计算
	2.b PART有merge, bootstrap, split, 重新计算
3. PS lock streamID


## TODO
1. 实现
2. rp实现valuelog的truncate(*)
3. 实现logstream分为2个不同的stream,一个可以在生成memtable后直接删除, 另一个长久保存(定期recycle或者EC化)
4. ps merge / split


### extent log format

```
+---------+-----------+-----------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Payload   |
+---------+-----------+-----------+--- ... ---+
```

### WAL

WAL write wal and extent at the same time.


### EC



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


### stream layer data recovery

ETCD存储结构

1. 可管理
2. 处理recovery Node失败的情况
3. 同一个extent,只有一个进程在恢复



准备一个SET, 持久化在etcd里, 里面/recoveryTasks是node已经接受的任务, 还没有明确完成
只有在df之后, 才标记完成



1. 数据恢复

准备一个SET, 持久化在etcd里, 里面/recoveryTasks是需要执行的的recovery任务,
只有在成功恢复了一个extent之后, 才在etcd里面删除这个task, 同时在leader的内存里面
存储taskPool, 这个和SET保持一致

/recoveryTasks/extentID.tsk => recoveryTask

这个SET有2个来源
1. stream manager scan得到()
2. 由node主动提出, 比如node有一个disk failure,
   2.a 如果是全盘raid5, raid5自行处理(目前选择)
   2.b node自己处理, 把extent转移到其他硬盘, 这种情况不需要修改元数据
   2.c node自己处理, 其他硬盘容量不够, node提一个api, 请上层处理

routineDispatchTask(每分钟一次)
	-> 如果存在extent的node是dead状态
	   -> routineDispatchTask
	   1. lock dispatchlock
	   2. taskPool deduplicat
	   3. 再读extentInfo一遍, 确认
	   4. 发recovery task任务
	   5. unlock dispatchlock

其中2跟一个race condition相关, 主要好处是避免过多的dlock


df任务(每分钟一次)
	-> 读taskPool
	   1. 如果node上面有任务, 询问任务状态
	   2. 如果任务已经完成
	   3. lock dispatchlock
	   4. 修改taskPool, 修改ETCD
	   5. unlock dispatchLock

Node:

咨询是否删除某个extent(一段时间一次)
1. node询问某一个extent是否可以删除
2. sm_manager检查extents和dispatchLock taskPool检查 dispatchUnlock


RequireRecovery
1. 如果自己的load太高, 拒绝
2. 读取最新extent, 判断是否合理(extentInfo里面存在replaceID), 否则拒绝
2. 在指定文件系统中生成tmp文件
3. runRecoveryTask,返回已经接收任务
    -> runRecoveryTask  拷贝一个副本到本node
	如果全部完成, 修改tmp文件成为ext文件,并且加入到extent中


//ETCD LOCK
//https://etcd.io/docs/next/learning/why/#notes-on-the-usage-of-lock-and-lease
//https://github.com/etcd-io/etcd/issues/11457

重复提交task

1.==========如果"创建task"在"更新extentInfo(内存)"之前运行

manager                  manager_service(df routine)


创建task(如果有dead node)
                    	 更新extentInfo (etcd)
                	     删除etcd task    (etcd)
						 更新extentInfo(内存)/删除recovies
create task(etcd)       

## stream lock
stream增加stream lock, 在分布式环境保证同时只有一个writer
写入包括:
1. append
2. seal/
3. alloc new extent
4. truncate

1. append不经过manager, 所以需要用revision当方式, 当etcd得到某一个stream的锁以后, 生成
的revision一定比之前的高
2. seal虽然经过manager, 但是manager的方式中是先seal extent, 再修改etcd数据, 所以也需要
检查revision和检查ownerkey, 如果检查revision成功, 检查ownerkey失败, 会在node留下extent垃圾
3. alloc new extent需要在manager校验client是lock的owner
4. truncate还没做

stream lock也要有超时


(a) transactions/second, 
(b) average pending transaction count
(c) throttling rate
(d) CPU usage
(e) network usage
(f) request latency
(g) data size of the RangePartition

1. offload
2. split due to cpu load or size of row or blob streams


At the start of a partition load, the partition server sends a “check for commit length” to the primary EN of the last extent of these two streams. This checks whether all the replicas are available and that they all have the same length. 
If not, the extent is sealed and reads are only performed, during partition load, against a replica sealed by the SM. 
This ensures that the partition load will see all of its data and the exact same view, even if we were to repeatedly load the same partition reading from different sealed replicas for the last extent of the stream


 This ensures that once an extent is sealed, all its available replicas (the ones the SM can eventually reach) are bitwise identical.

 1. Once a record is appended and acknowledged back to the client, any later reads of that record from any replica will see the same data (the data is immutable).
2. Once an extent is sealed, any reads from any sealed replica will always see the same contents of the extent.	


If an EN was not reachable by the SM during the sealing process but later becomes reachable
the SM will force the EN to synchronize the given extent to the chosen commit length

2. node增加过期extent检查
5. 增加删除stream