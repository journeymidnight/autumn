use serde::{Deserialize, Serialize};
use crate::errors::ExtentError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: u64,
    pub dirs: Vec<String>,
    pub wal_dir: String,
    pub listen_url: String,
    pub sm_urls: Vec<String>,
    pub etcd_urls: Vec<String>,
    pub trace_sampler: f64,
}

impl NodeConfig {
    pub fn new() -> Result<Self, ExtentError> {
        // In a real implementation, this would read from config file or environment
        Ok(Self {
            id: 1,
            dirs: vec!["/tmp/extent_node/disk1".to_string()],
            wal_dir: "/tmp/extent_node/wal".to_string(),
            listen_url: "0.0.0.0:9000".to_string(),
            sm_urls: vec!["http://localhost:8080".to_string()],
            etcd_urls: vec!["http://localhost:2379".to_string()],
            trace_sampler: 0.1,
        })
    }
    
    pub fn from_args() -> Result<Self, ExtentError> {
        // Parse command line arguments
        // This is a simplified version
        Self::new()
    }
}