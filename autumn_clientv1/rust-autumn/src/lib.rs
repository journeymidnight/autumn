mod pspb;

use std::io::ErrorKind;

use crate::pspb::RegionInfo;
use etcd_client::Client;
use prost::Message;
use pspb::partition_kv_client::PartitionKvClient;
use std::sync::{Arc, Mutex};
use tokio::select;
use tonic::transport::Channel;

use std::cmp::Ordering;
use std::collections::HashMap;

pub struct AutumnLib {
    inner: Arc<Mutex<AutumnLibInner>>, //shared between threads
    conns: Arc<Mutex<HashMap<String, PartitionKvClient<Channel>>>>,
}
struct AutumnLibInner {
    region_list: Vec<pspb::RegionInfo>,
    ps_details: HashMap<u64, pspb::PsDetail>,
}

impl AutumnLib {
    fn convert_to_region_list(regions: pspb::Regions) -> Vec<RegionInfo> {
        let mut regions_list = Vec::new();
        for (_, r) in regions.regions {
            regions_list.push(r)
        }
        //sort list by startKey of regionInfo
        regions_list.sort_by(|a, b| {
            if let Some(aa) = &a.rg {
                if let Some(bb) = &b.rg {
                    return aa.start_key.cmp(&bb.start_key);
                }
                return std::cmp::Ordering::Greater;
            }
            if b.rg.is_some() {
                return std::cmp::Ordering::Less;
            }
            return std::cmp::Ordering::Equal;
        });

        return regions_list;
    }

    //param: list of strings
    //FIXME:
    pub async fn connect(list: Vec<&str>) -> Result<Self, std::io::Error> {
        let mut etcd_client = Client::connect(list, None).await.unwrap();

        let res = etcd_client.get("regions/config", None).await.unwrap();

        if res.kvs().len() != 1 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                "invalid regions/config",
            ));
        }
        let regions = pspb::Regions::decode(res.kvs()[0].value()).unwrap();
        let regions_list = AutumnLib::convert_to_region_list(regions);
        let watch_region_from_version =
            etcd_client::WatchOptions::new().with_start_revision(res.header().unwrap().revision());

        let res = etcd_client
            .get(
                "PSSERVER/",
                Some(etcd_client::GetOptions::new().with_range("PSSERVER0")),
            )
            .await
            .unwrap();

        if res.kvs().len() != 1 {
            return Err(std::io::Error::new(ErrorKind::Other, "invalid PSSERVER/"));
        }

        let watch_server_from_version =
            etcd_client::WatchOptions::new().with_start_revision(res.header().unwrap().revision());
        let mut ps_details = HashMap::new();
        for kv in res.kvs() {
            let detail = pspb::PsDetail::decode(kv.value()).unwrap();
            ps_details.insert(detail.psid, detail);
        }

        let (_, mut watchRegionStream) = etcd_client
            .watch("regions/config", Some(watch_region_from_version))
            .await
            .unwrap();
        let (_, mut watchPSStream) = etcd_client
            .watch("PSSERVER/", Some(watch_server_from_version))
            .await
            .unwrap();

        let inner = Arc::new(Mutex::new(AutumnLibInner {
            region_list: regions_list,
            ps_details: ps_details,
        }));

        let x = inner.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    ret = watchRegionStream.message() => {
                        // perform region decode here.
                        match ret{
                            Ok(ret) => {
                                if let Some(msg) = ret {
                                    if msg.canceled(){
                                        println!("watch canceled");
                                        return;
                                    }
                                    let events = msg.events();
                                    let num = events.len();
                                    if num == 0 {
                                        continue;
                                    }
                                    // use the last event
                                    let last_event = &events[num-1];

                                    let regions = pspb::Regions::decode(last_event.kv().unwrap().value()).unwrap();
                                    let regions_list = AutumnLib::convert_to_region_list(regions);
                                    //update region list
                                    //println!("update region list {:?}", regions_list);
                                    x.lock().unwrap().region_list = regions_list;
                                }
                            }
                            Err(err) => {
                                println!("watch_region: failed to receive region watch message: {}", err);
                            }
                        }
                    }
                };
            }
        });

        let y = inner.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    ret = watchPSStream.message() => {
                        // perform psDetail decode here.
                        match ret{
                            Ok(ret) => {
                                if let Some(msg) = ret {
                                    for event in msg.events() {
                                        match event.kv() {
                                            Some(kv) => {
                                                let detail = pspb::PsDetail::decode(kv.value()).unwrap();
                                                y.lock().unwrap().ps_details.insert(detail.psid, detail);
                                            }
                                            None => {
                                                println!("watch_ps: failed to receive psDetail watch message: {:?}", event);
                                            }
                                        }
                                    }
                                }
                                //none
                            }
                            Err(err) => {
                                println!("watch_ps: failed to receive ps watch message: {}", err);
                            }
                        }
                    }
                };
            }
        });

        return Ok(AutumnLib {
            inner: inner,
            conns: Arc::new(Mutex::new(HashMap::new())),
        });
    }

    //get_region returns part_id and ps_id
    fn get_region(self: &Self, key: &str) -> Option<(u64, u64)> {
        let regions = &self.inner.lock().unwrap().region_list;

        if regions.len() == 0 {
            println!("no region");
            return None;
        }

        //std library of rust do not support binary search??
        let mut lo = 0;
        let mut hi = regions.len();

        fn matchFunc(regions: &Vec<RegionInfo>, i: usize, key: &str) -> bool {
            // if len(sortedRegions[i].Rg.EndKey) == 0 {
            //     return true
            // }
            // return bytes.Compare(sortedRegions[i].Rg.EndKey, key) > 0
            let end_key = &regions[i].rg.as_ref().unwrap().end_key[..];
            if end_key.len() == 0 {
                return true;
            }
            //cmpare end_key with key
            return end_key.cmp(key.as_bytes()) == Ordering::Greater;
        }

        while lo < hi {
            let mid = (lo + hi) / 2;
            if !matchFunc(regions, mid, key) {
                lo = mid + 1
            } else {
                hi = mid
            }
        }

        if lo == regions.len() {
            return None;
        }

        let ps_id = &regions[lo].psid;
        return Some((regions[lo].part_id, regions[lo].psid));
    }

    pub async fn delete(self: &Self, key: &str) -> Result<(), std::io::Error> {
        if let Some((part_id, ps_id)) = self.get_region(key) {
            let addr = self.get_ps_addr(ps_id).await;

            match self.get_conn(&addr).await {
                Ok(mut client) => {
                    let req = pspb::DeleteRequest {
                        key: key.into(),
                        partid: part_id,
                    };

                    match client.delete(req).await {
                        Ok(res) => {
                            return Ok(());
                        }
                        Err(err) => {
                            return Err(std::io::Error::new(ErrorKind::Other, err));
                        }
                    }
                }
                Err(err) => {
                    return Err(std::io::Error::new(ErrorKind::Other, err));
                }
            }
        } else {
            return Err(std::io::Error::new(ErrorKind::Other, "no region"));
        }
    }

    pub async fn put(self: &Self, key: &str, value: Vec<u8>) -> Result<(), std::io::Error> {
        if let Some((part_id, ps_id)) = self.get_region(key) {
            let addr = self.get_ps_addr(ps_id).await;

            match self.get_conn(&addr).await {
                Ok(mut client) => {
                    let req = pspb::PutRequest {
                        key: key.into(),
                        value: value,
                        partid: part_id,
                        expires_at: 0,
                    };

                    match client.put(req).await {
                        Ok(res) => {
                            return Ok(());
                        }
                        Err(err) => {
                            return Err(std::io::Error::new(ErrorKind::Other, err));
                        }
                    }
                }
                Err(err) => {
                    return Err(std::io::Error::new(ErrorKind::Other, err));
                }
            }
        } else {
            return Err(std::io::Error::new(ErrorKind::Other, "no region"));
        }
    }

    fn clone_region(self: &Self) -> Vec<RegionInfo> {
        let regions = self.inner.lock().unwrap().region_list.clone();
        return regions;
    }

    pub async fn range(
        self: &Self,
        prefix: &str,
        start: &str,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, std::io::Error> {
        //clone region list
        let regions = self.clone_region();
        let mut limit = limit;
        if regions.len() == 0 {
            return Err(std::io::Error::new(ErrorKind::Other, "no region"));
        }

        let more = false;
        let mut result = Vec::new();
        let (part_id, ps_id) = match self.get_region(start) {
            Some(x) => x,
            None => return Err(std::io::Error::new(ErrorKind::Other, "no region")),
        };

        for i in 0..regions.len() {
            if limit <= 0 {
                break;
            }
            if let Some(range) = &regions[i].rg {
                if !range.start_key.starts_with(prefix.as_bytes()) {
                    break;
                }
            } else {
                break;
            }

            let addr = self.get_ps_addr(ps_id).await;
            match self.get_conn(&addr).await {
                Ok(mut client) => {
                    let req = pspb::RangeRequest {
                        prefix: prefix.into(),
                        partid: regions[i].part_id,
                        start: start.into(),
                        limit: limit,
                    };
                    match client.range(req).await {
                        Ok(res) => {
                            let keys = res.into_inner().keys;
                            limit -= keys.len() as u32;
                            result.extend(keys);
                        }
                        Err(e) => {
                            return Err(std::io::Error::new(ErrorKind::Other, e));
                        }
                    }
                }
                Err(err) => {
                    return Err(std::io::Error::new(ErrorKind::Other, err));
                }
            }
        }

        return Ok(result);
    }

    //head return len of key
    pub async fn head(self: &Self, key: &str) -> Result<u32, std::io::Error> {
        if let Some((part_id, ps_id)) = self.get_region(key) {
            let addr = self.get_ps_addr(ps_id).await;

            match self.get_conn(&addr).await {
                Ok(mut client) => {
                    let req = pspb::HeadRequest {
                        key: key.into(),
                        partid: part_id,
                    };

                    match client.head(req).await {
                        Ok(res) => Ok(res.into_inner().info.unwrap().len),
                        Err(err) => {
                            return Err(std::io::Error::new(ErrorKind::Other, err));
                        }
                    }
                }
                Err(err) => {
                    return Err(std::io::Error::new(ErrorKind::Other, err));
                }
            }
        } else {
            return Err(std::io::Error::new(ErrorKind::Other, "no region"));
        }
    }

    pub async fn get(self: &Self, key: &str) -> Result<Vec<u8>, std::io::Error> {
        if let Some((part_id, ps_id)) = self.get_region(key) {
            let addr = self.get_ps_addr(ps_id).await;

            match self.get_conn(&addr).await {
                Ok(mut client) => {
                    let req = pspb::GetRequest {
                        key: key.into(),
                        partid: part_id,
                    };

                    match client.get(req).await {
                        Ok(res) => {
                            return Ok(res.into_inner().value);
                        }
                        Err(err) => {
                            return Err(std::io::Error::new(ErrorKind::Other, err));
                        }
                    }
                }
                Err(err) => {
                    return Err(std::io::Error::new(ErrorKind::Other, err));
                }
            }
        } else {
            return Err(std::io::Error::new(ErrorKind::Other, "no region"));
        }
    }

    async fn get_conn(
        self: &Self,
        addr: &str,
    ) -> Result<
        pspb::partition_kv_client::PartitionKvClient<tonic::transport::Channel>,
        tonic::transport::Error,
    > {
        let mut conns = self.conns.lock().unwrap();
        if let Some(conn) = conns.get(addr) {
            return Ok(conn.clone());
        }
        //tonic requires a schema for address
        match tonic::transport::Endpoint::new(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
        {
            Ok(client) => {
                let conn = pspb::partition_kv_client::PartitionKvClient::new(client);
                conns.insert(addr.to_string(), conn.clone());
                return Ok(conn);
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    async fn get_ps_addr(self: &Self, ps_id: u64) -> String {
        loop {
            match self.inner.lock().unwrap().ps_details.get(&ps_id) {
                Some(ps_detail) => return ps_detail.address.to_owned(),
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
    }
}
