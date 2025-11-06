use crate::errors::ExtentError;
use crate::extent::{Extent, DiskFS, Wal, ExtentManager};
use crate::node::NodeConfig;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{info, warn, error, debug};

#[derive(Clone)]
pub struct ExtentOnDisk {
    pub extent: Arc<Extent>,
    pub disk_id: u64,
}

pub struct ExtentNode {
    pub node_id: u64,
    pub listen_url: String,
    pub disk_fss: Arc<RwLock<HashMap<u64, Arc<DiskFS>>>>,
    pub wal: Option<Arc<Wal>>,
    pub extent_map: Arc<RwLock<HashMap<u64, ExtentOnDisk>>>,
    pub extent_manager: Arc<ExtentManager>,
}

impl ExtentNode {
    pub fn new(config: &NodeConfig) -> Result<Arc<Self>, ExtentError> {
        let mut disk_fss = HashMap::new();
        
        // Initialize disk filesystems
        for (i, dir) in config.dirs.iter().enumerate() {
            let disk_id = i as u64 + 1;
            let path = PathBuf::from(dir);
            match DiskFS::new(disk_id, path) {
                Ok(disk_fs) => {
                    disk_fss.insert(disk_id, Arc::new(disk_fs));
                    info!("Loaded disk {} at {}", disk_id, dir);
                }
                Err(e) => {
                    warn!("Failed to load disk {}: {}", dir, e);
                }
            }
        }
        
        if disk_fss.is_empty() {
            return Err(ExtentError::Config("No available disks".to_string()));
        }
        
        // Initialize WAL if wal_dir is specified
        let wal = if !config.wal_dir.is_empty() {
            match Wal::open(&config.wal_dir, || {
                // Sync callback - in real implementation would sync all disks
            }) {
                Ok(wal) => Some(Arc::new(wal)),
                Err(e) => {
                    warn!("Failed to initialize WAL: {}", e);
                    None
                }
            }
        } else {
            None
        };
        
        // 创建ExtentManager，传入WAL引用
        let extent_manager = Arc::new(ExtentManager::with_wal(wal.clone()));
        
        let node = Arc::new(Self {
            node_id: config.id,
            listen_url: config.listen_url.clone(),
            disk_fss: Arc::new(RwLock::new(disk_fss)),
            wal,
            extent_map: Arc::new(RwLock::new(HashMap::new())),
            extent_manager,
        });
        
        Ok(node)
    }
    
    pub fn get_extent(&self, extent_id: u64) -> Option<ExtentOnDisk> {
        let extent_map = self.extent_map.read();
        extent_map.get(&extent_id).cloned()
    }
    
    pub fn set_extent(&self, extent_id: u64, extent_on_disk: ExtentOnDisk) {
        let mut extent_map = self.extent_map.write();
        extent_map.insert(extent_id, extent_on_disk.clone());
        
        // 异步添加到extent管理器
        let extent_manager = self.extent_manager.clone();
        let extent = extent_on_disk.extent.clone();
        tokio::spawn(async move {
            if let Err(e) = extent_manager.add_extent(extent_id, extent).await {
                error!("Failed to add extent {} to async manager: {}", extent_id, e);
            }
        });
    }
    
    pub fn remove_extent(&self, extent_id: u64) -> Option<ExtentOnDisk> {
        let mut extent_map = self.extent_map.write();
        let result = extent_map.remove(&extent_id);
        
        if result.is_some() {
            // 异步从extent管理器中移除
            let extent_manager = self.extent_manager.clone();
            tokio::spawn(async move {
                if let Err(e) = extent_manager.remove_extent(extent_id).await {
                    error!("Failed to remove extent {} from async manager: {}", extent_id, e);
                }
            });
        }
        
        result
    }
    
    pub fn choose_disk_to_alloc(&self) -> Option<u64> {
        let disk_fss = self.disk_fss.read();
        for (disk_id, disk_fs) in disk_fss.iter() {
            if disk_fs.is_online() {
                return Some(*disk_id);
            }
        }
        None
    }
    
    pub fn load_extents(&self) -> Result<(), ExtentError> {
        let disk_fss = self.disk_fss.read();
        
        let register_ext = |path: PathBuf, disk_id: u64| {
            if let Ok(extent) = Extent::open(&path) {
                let extent_on_disk = ExtentOnDisk {
                    extent: extent.clone(),
                    disk_id,
                };
                self.set_extent(extent.id(), extent_on_disk);
                debug!("Loaded extent {} from disk {}", extent.id(), disk_id);
            } else {
                error!("Failed to open extent at {:?}", path);
            }
        };
        
        let register_copy = |path: PathBuf, disk_id: u64| {
            info!("Found recovery copy at {:?}", path);
            // Handle recovery task - simplified for now
        };
        
        // Load extents from all disks
        for disk_fs in disk_fss.values() {
            disk_fs.load_extents(register_ext, register_copy)?;
        }
        
        // Replay WAL if available
        if let Some(wal) = &self.wal {
            wal.replay(|extent_id, start, revision, blocks| {
                if let Some(extent_on_disk) = self.get_extent(extent_id) {
                    if let Err(e) = extent_on_disk.extent.recovery_data(start, revision, blocks) {
                        error!("Failed to replay WAL for extent {}: {:?}", extent_id, e);
                    }
                } else {
                    warn!("Extent {} not found for WAL replay", extent_id);
                }
            })?;
        }
        
        Ok(())
    }
    
    pub async fn append_with_wal(
        &self,
        extent_id: u64,
        revision: i64,
        blocks: Vec<Bytes>,
        must_sync: bool,
    ) -> Result<(Vec<u32>, u32), ExtentError> {
        if !must_sync {
            // 直接通过异步管理器写入，不同步
            return self.extent_manager.append_blocks(extent_id, revision, blocks, false).await;
        }
        
        // 检查是否应该使用WAL
        let total_size: usize = blocks.iter().map(|b| b.len()).sum();
        let should_use_wal = self.wal.is_some() && total_size <= 2 * 1024 * 1024; // 2MB
        
        if !should_use_wal {
            // 强制同步写入
            return self.extent_manager.append_blocks(extent_id, revision, blocks, true).await;
        }
        
        // 使用WAL + 异步extent写入
        // 获取extent的当前commit length
        let start = self.extent_manager.get_commit_length(extent_id).await?;
        
        // 写入WAL (通过extent_manager的I/O线程)
        self.extent_manager.write_wal(extent_id, start, revision, blocks.clone()).await?;
        
        // 写入extent (异步，不同步到磁盘，因为WAL已经保证持久性)
        self.extent_manager.append_blocks(extent_id, revision, blocks, false).await
    }
    
    pub fn shutdown(&self) {
        info!("Shutting down extent node {}", self.node_id);
        
        // Close all extents
        let extent_map = self.extent_map.read();
        for (extent_id, _) in extent_map.iter() {
            info!("Closing extent {}", extent_id);
            // Extents will be closed when dropped
        }
        
        info!("Extent node shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::Extent;
    use bytes::Bytes;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    
    async fn create_test_node() -> (Arc<ExtentNode>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().join("data");
        let wal_path = temp_dir.path().join("wal");
        
        std::fs::create_dir_all(&data_path).unwrap();
        std::fs::create_dir_all(&wal_path).unwrap();
        
        let config = NodeConfig {
            id: 1,
            dirs: vec![data_path.to_string_lossy().to_string()],
            wal_dir: wal_path.to_string_lossy().to_string(),
            listen_url: "127.0.0.1:9000".to_string(),
            sm_urls: vec!["127.0.0.1:3401".to_string()],
            etcd_urls: vec!["127.0.0.1:2379".to_string()],
            trace_sampler: 0.0,
        };
        
        let node = ExtentNode::new(&config).unwrap();
        
        // 创建测试extent
        let extent_path = data_path.join("test_extent.ext");
        let extent = Extent::create(&extent_path, 12345).unwrap();
        let extent_on_disk = ExtentOnDisk {
            extent,
            disk_id: 1,
        };
        node.set_extent(12345, extent_on_disk);
        
        (node, temp_dir)
    }
    
    fn generate_test_data(size: usize) -> Vec<Bytes> {
        let data = vec![0u8; size];
        vec![Bytes::from(data)]
    }
    
    #[tokio::test]
    async fn test_append_with_wal_basic() {
        let (node, _temp_dir) = create_test_node().await;
        
        let blocks = generate_test_data(4096);
        let result = node.append_with_wal(12345, 1, blocks, true).await;
        
        assert!(result.is_ok());
        let (offsets, end) = result.unwrap();
        assert!(!offsets.is_empty());
        assert!(end > 0);
    }
    
    #[tokio::test]
    async fn bench_append_with_wal_small_blocks() {
        let (node, _temp_dir) = create_test_node().await;
        
        // 测试不同大小的数据块
        let sizes = [1024, 4096, 16384, 65536]; // 1KB到64KB
        let iterations = 1000i64;
        
        for size in sizes {
            let start_time = Instant::now();
            
            for i in 0..iterations {
                let blocks = generate_test_data(size);
                let _ = node.append_with_wal(12345, i, blocks, true).await;
            }
            
            let elapsed = start_time.elapsed();
            let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
            let mb_per_sec = (size as i64 * iterations) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            
            println!(
                "Rust append_with_wal - Size: {}KB, Iterations: {}, Time: {:?}, Ops/sec: {:.2}, MB/sec: {:.2}",
                size / 1024,
                iterations,
                elapsed,
                ops_per_sec,
                mb_per_sec
            );
        }
    }
    
    #[tokio::test]
    async fn bench_append_with_wal_large_blocks() {
        let (node, _temp_dir) = create_test_node().await;
        
        // 测试大数据块 (>= 2MB，会跳过WAL)
        let sizes = [2 * 1024 * 1024, 4 * 1024 * 1024]; // 2MB, 4MB
        let iterations = 100i64;
        
        for size in sizes {
            let start_time = Instant::now();
            
            for i in 0..iterations {
                let blocks = generate_test_data(size);
                let _ = node.append_with_wal(12345, i, blocks, true).await;
            }
            
            let elapsed = start_time.elapsed();
            let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
            let mb_per_sec = (size as i64 * iterations) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            
            println!(
                "Rust append_with_wal (large) - Size: {}MB, Iterations: {}, Time: {:?}, Ops/sec: {:.2}, MB/sec: {:.2}",
                size / (1024 * 1024),
                iterations,
                elapsed,
                ops_per_sec,
                mb_per_sec
            );
        }
    }
    
    #[tokio::test]
    async fn bench_append_with_wal_sync_vs_nosync() {
        let (node, _temp_dir) = create_test_node().await;
        
        let size = 4096;
        let iterations = 1000i64;
        
        // 测试 must_sync = true
        let blocks = generate_test_data(size);
        let start_time = Instant::now();
        
        for i in 0..iterations {
            let blocks = blocks.clone();
            let _ = node.append_with_wal(12345, i, blocks, true).await;
        }
        
        let sync_elapsed = start_time.elapsed();
        
        // 测试 must_sync = false
        let start_time = Instant::now();
        
        for i in 0..iterations {
            let blocks = blocks.clone();
            let _ = node.append_with_wal(12345, i + iterations, blocks, false).await;
        }
        
        let nosync_elapsed = start_time.elapsed();
        
        println!(
            "Rust append_with_wal sync comparison - Size: {}KB, Iterations: {}",
            size / 1024,
            iterations
        );
        println!("  Sync=true:  {:?} ({:.2} ops/sec)", sync_elapsed, iterations as f64 / sync_elapsed.as_secs_f64());
        println!("  Sync=false: {:?} ({:.2} ops/sec)", nosync_elapsed, iterations as f64 / nosync_elapsed.as_secs_f64());
        println!("  Speed ratio: {:.2}x", nosync_elapsed.as_secs_f64() / sync_elapsed.as_secs_f64());
    }
    
    #[tokio::test]
    async fn bench_append_with_wal_concurrent() {
        let (node, _temp_dir) = create_test_node().await;
        
        let thread_counts = [1, 4, 8, 16];
        let operations_per_thread = 250i64;
        let size = 4096;
        
        for num_threads in thread_counts {
            let start_time = Instant::now();
            
            let mut handles = Vec::new();
            for thread_id in 0..num_threads {
                let node = node.clone();
                let handle = tokio::spawn(async move {
                    for i in 0..operations_per_thread {
                        let blocks = generate_test_data(size);
                        let extent_id = 12345;
                        let revision = thread_id as i64 * operations_per_thread + i;
                        let _ = node.append_with_wal(extent_id, revision, blocks, true).await;
                    }
                });
                handles.push(handle);
            }
            
            // 等待所有线程完成
            for handle in handles {
                handle.await.unwrap();
            }
            
            let elapsed = start_time.elapsed();
            let total_ops = num_threads as i64 * operations_per_thread;
            let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
            let mb_per_sec = (size as i64 * total_ops) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            
            println!(
                "Rust append_with_wal concurrent - Threads: {}, Ops/thread: {}, Total time: {:?}, Ops/sec: {:.2}, MB/sec: {:.2}",
                num_threads,
                operations_per_thread,
                elapsed,
                ops_per_sec,
                mb_per_sec
            );
        }
    }
    
    #[tokio::test]
    async fn bench_append_with_wal_latency_distribution() {
        let (node, _temp_dir) = create_test_node().await;
        
        let iterations = 1000i64;
        let size = 4096;
        let mut latencies = Vec::with_capacity(iterations as usize);
        
        for i in 0..iterations {
            let blocks = generate_test_data(size);
            let start = Instant::now();
            
            let _ = node.append_with_wal(12345, i, blocks, true).await;
            
            let latency = start.elapsed();
            latencies.push(latency);
        }
        
        // 排序计算分位数
        latencies.sort();
        
        let p50 = latencies[latencies.len() * 50 / 100];
        let p90 = latencies[latencies.len() * 90 / 100];
        let p95 = latencies[latencies.len() * 95 / 100];
        let p99 = latencies[latencies.len() * 99 / 100];
        let avg = Duration::from_nanos(
            (latencies.iter().map(|d| d.as_nanos()).sum::<u128>() / latencies.len() as u128).try_into().unwrap()
        );
        
        println!("Rust append_with_wal latency distribution - Size: {}KB, Iterations: {}", size / 1024, iterations);
        println!("  Average: {:?}", avg);
        println!("  P50: {:?}", p50);
        println!("  P90: {:?}", p90);
        println!("  P95: {:?}", p95);
        println!("  P99: {:?}", p99);
        println!("  Min: {:?}", latencies[0]);
        println!("  Max: {:?}", latencies[latencies.len() - 1]);
    }
}