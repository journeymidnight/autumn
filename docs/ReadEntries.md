nodeService API

ReadEntries EntryInfo:

replayLog && big => {(value=nil, extentID/offset) MUST , estimatedsize COULD)
replayLog && small => {extentID/offset/estimatedsize COULD)
gc        && big => {value=nil, (estimatedsize,key, extentID, offset) MUST}
gc        && small => {all block, key, value = nil, estimatedsize MUST}


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

