#/usr/bin/python3
import pspb_pb2_grpc as pspb_grpc
import pspb_pb2 as pspb
import etcd3
import threading
import grpc

def _binary_search(array, matchFunc):
    i = 0
    j = len(array)
    while i < j:
        mid = (i + j) >> 1
        if not matchFunc(array[mid]):
            i = mid + 1
        else:
            j = mid
    return i

class AutumnLib:
    def __init__(self):
        self.psDetails = {}
        self.conns = {}
        self.mutex = threading.Lock() #FIXME: RWlock

        self.cons = {}
        self.consLock = threading.Lock()

    def _save_regions(self, regions):
        sorted_regions=sorted(regions.values(), key=lambda x: x.rg.startKey)
        with self.mutex:
            self.regions = sorted_regions

    def _update_regions_config(self, events):
        if type(events) is not etcd3.watch.WatchResponse:
            return
        if len(events.events) == 0:
            return
        event = events.events[-1]#skip to the last event
        regions = pspb.Regions()
        regions.ParseFromString(event.value)
        self._save_regions(regions.regions)
    

    def _getPSAddr(self, psid):
        with self.mutex:
            return self.psDetails[psid].address
    
    def _getRegions(self):
        with self.mutex:
            return self.regions

    def _getConn(self, addr):
        with self.consLock:
            if addr in self.cons:
                return self.cons[addr]
        conn = grpc.insecure_channel(target=addr,
        options=[
        ('grpc.max_send_message_length', 33<<20),
        ('grpc.max_receive_message_length', 33<<20),
        ('grpc.max_reconnect_backoff_ms', 1000),
        ])
        with self.consLock:
            self.cons[addr] = conn
        return conn

    def _update_ps_config(self, events):
        if type(events) is not etcd3.watch.WatchResponse:
            return
        for event in events.events:
            if type(event) is etcd3.events.PutEvent:
                psDetail = pspb.PSDetail()
                psDetail.ParseFromString(event.value)
                with self.mutex:
                    self.psDetails[psDetail.PSID] = psDetail
            elif type(event) is etcd3.events.DeleteEvent:
                #parse PSID from event.key, example b"PSSERVER/10"
                #convert psid to int
                psid = int(str(event.key, "utf8").split("/")[1])
                with self.mutex:
                    del self.psDetails[psid]
            else:
                print(event)

    def Put(self, key , value):
        if len(key) == 0 or len(value) == 0:
            return
        if len(value) > (32<<20):
            print("value too long")
            return
        sortedRegion = self._getRegions()
        if len(sortedRegion) == 0:
            return


        idx = _binary_search(sortedRegion, lambda x: 
            len(x.rg.endKey) == 0 or x.rg.endKey > key
        )
        conn = self._getConn(self._getPSAddr(sortedRegion[idx].PSID))
        stub = pspb_grpc.PartitionKVStub(conn)
        return stub.Put(pspb.PutRequest(key=key,value=value,partid=sortedRegion[idx].PartID))


    def List(self, start, prefix, limit):
        sortedRegion = self._getRegions()
        if len(sortedRegion) == 0:
            return None
        #loop over all regions
        try:
            for region in sortedRegion:
                conn = self._getConn(self._getPSAddr(region.PSID))
                stub = pspb_grpc.PartitionKVStub(conn)
                res = stub.Range(pspb.RangeRequest(
                    partid=region.PartID,
                    limit = limit,
                    start=start,
                    prefix=prefix
                ))
                #append res to list
                for key in res.keys:
                    yield str(key, "utf-8")
                #yield str(res.keys, "utf-8")
        except Exception as e:
            print(e)
        
        

    def Get(self, key):
        sortedRegion = self._getRegions()
        if len(sortedRegion) == 0:
            return None
        idx = _binary_search(sortedRegion, lambda x: 
            len(x.rg.endKey) == 0 or x.rg.endKey > key
        )
        conn = self._getConn(self._getPSAddr(sortedRegion[idx].PSID))
        stub = pspb_grpc.PartitionKVStub(conn)
        try:
            ret = stub.Get(pspb.GetRequest(key=key,partid=sortedRegion[idx].PartID))
            return ret
        except Exception as e:
            #print "key is partID"
            print("key is %s" % key)
            return None

    def Connect(self):
        try:
            self.etcdClient = etcd3.client(grpc_options={
                '"grpc.max_reconnect_backoff_ms': 1000,
            }.items())

            max_revision = 0
            data, meta = self.etcdClient.get("regions/config")
            regions = pspb.Regions()

            max_revision = max(max_revision, meta.mod_revision)

            regions.ParseFromString(data)
            self._save_regions(regions.regions)


            tuples = self.etcdClient.get_prefix("PSSERVER/")
            for item in tuples:
                data = item[0]
                meta = item[1]
                psDetail = pspb.PSDetail()
                psDetail.ParseFromString(data)
                self.psDetails[psDetail.PSID] = psDetail
                max_revision = max(max_revision, meta.mod_revision)
            
            #start watch 

            self.etcdClient.add_watch_callback("regions/config", self._update_regions_config, start_revision=max_revision)  
            self.etcdClient.add_watch_callback("PSSERVER/", self._update_ps_config, "PSSERVER0", start_revision=max_revision)    
        except Exception as e:
            print(e.with_traceback())


if __name__ == "__main__":
    lib = AutumnLib()
    lib.Connect()
