nodeService API

ReadEntries EntryInfo:

replayLog && big => {(value=nil, extentID/offset) MUST , estimatedsize COULD)
replayLog && small => {extentID/offset/estimatedsize COULD)
gc        && big => {value=nil, (estimatedsize,key, extentID, offset) MUST}
gc        && small => {nil}//{all block, key, value = nil, estimatedsize MUST}


1. 如何决定最新的table? 写入(seqNum + 1), 即使table里面都是old gc数据, 也可以保证最新
2. gc, version不变, 只要当version一样, offset/extentID一样时, 才重新写入
3. delete的问题:

hasOverlap keep delete
or no

table1 table2 table3
Wx1    Wx2    Dx3

merge 2&3 => Dx3
merge 1&n => Dx3 

注意用skipkey
4. merge iterator也要处理duplicate key的情况, 同一个iter里面有dup, 和不同iter直接有dup

5. 场景

mt : key:300

tables: key:100
tables: key:50

读valuelog, 发现key:50, 
如果不支持snapshot和多version, 查询到key:300, 就可以discard这个key:50,
但是如果支持多version, (所有的get请求,需要读全量mt, imm, 全tables) key:50有可能又插入到了mt中, 
但是由于我们的所有tables,mt,imm有序, 所以新key:50, 会在get比老key:50靠前

多verison情况下
1. discard和搜索的逻辑不同
2. compaction的时候逻辑不同
3. get的逻辑不通, 再搜索完mt和imm之后, 必须搜索所有tables