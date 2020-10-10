## API

对外提供GET/PUT/DELETE object的功能

## stream layer

提供Append Only的Stream, 只有partion layer使用

### extent结构

```
extent头(512字节)
	magic number (8字节)
	extent ID    (8字节)
block头 (512字节)
	checksum    (4字节)
	blocklength (4字节) // 所以extent理论最大4G, 实际现在2G
block数据
	数据  (4k对齐)

extent是否seal, 存储在文件系统的attr里面
"seal"=>"true"
```

主要API:
1. OpenExtent
2. AppendBlock
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
4. CommitLenght (由stream manager调用)
5. Seal (由stream manager调用)

主要内存结构:
1. extendID => localFileName (在启动时打开所有extent的fd)
2. extentID => [副本1地址, 副本2地址] //cache从stream manager查询的数据


### stream manager

constructing...

实现采用embed etcd

API:

内存结构:

## partion layer

