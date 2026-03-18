use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use autumn_proto::autumn::{DiskInfo, ExtentInfo, NodeInfo, PartitionMeta, RegionInfo, StreamInfo};
use parking_lot::RwLock;

use crate::{AppError, AppResult};

#[derive(Debug, Default)]
pub struct MetadataState {
    pub next_id: u64,
    pub streams: HashMap<u64, StreamInfo>,
    pub extents: HashMap<u64, ExtentInfo>,
    pub nodes: HashMap<u64, NodeInfo>,
    pub disks: HashMap<u64, DiskInfo>,
    pub owner_revisions: HashMap<String, i64>,
    pub next_revision: i64,
    pub partitions: HashMap<u64, PartitionMeta>,
    pub ps_nodes: HashMap<u64, String>,
    pub regions: BTreeMap<u64, RegionInfo>,
}

impl MetadataState {
    pub fn alloc_ids(&mut self, count: u64) -> (u64, u64) {
        let start = self.next_id.max(1);
        let end = start + count;
        self.next_id = end;
        (start, end)
    }

    pub fn acquire_owner_lock(&mut self, key: &str) -> i64 {
        if let Some(v) = self.owner_revisions.get(key) {
            return *v;
        }
        self.next_revision += 1;
        let rev = self.next_revision;
        self.owner_revisions.insert(key.to_string(), rev);
        rev
    }

    pub fn ensure_owner_revision(&self, key: &str, revision: i64) -> AppResult<()> {
        match self.owner_revisions.get(key) {
            Some(v) if *v == revision => Ok(()),
            Some(v) => Err(AppError::Precondition(format!(
                "owner_key={key} revision mismatch, expected {v}, got {revision}"
            ))),
            None => Err(AppError::Precondition(format!(
                "owner_key={key} does not exist"
            ))),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataStore {
    pub inner: Arc<RwLock<MetadataState>>,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetadataState {
                next_id: 1,
                next_revision: 0,
                ..MetadataState::default()
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_ids_monotonic() {
        let mut s = MetadataState::default();
        let (a1, a2) = s.alloc_ids(2);
        assert_eq!((a1, a2), (1, 3));
        let (b1, b2) = s.alloc_ids(3);
        assert_eq!((b1, b2), (3, 6));
    }

    #[test]
    fn owner_lock_revision_validation() {
        let mut s = MetadataState::default();
        let rev = s.acquire_owner_lock("lock/a");
        assert!(s.ensure_owner_revision("lock/a", rev).is_ok());
        assert!(s.ensure_owner_revision("lock/a", rev + 1).is_err());
        assert!(s.ensure_owner_revision("lock/b", 1).is_err());
    }
}
