use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use tempfile::TempDir;

// Import from the local crate
use extent_node_rust::extent::Extent;

fn create_test_data(size: usize) -> Vec<Bytes> {
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    vec![Bytes::from(data)]
}

fn bench_append_blocks_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_blocks_by_size");

    let sizes = [1024, 4096, 16384]; // 1KB, 4KB, 16KB

    for size in sizes.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // 在迭代外创建extent，避免重复创建的开销
            let tmpdir = TempDir::new().unwrap();
            let extent_path = tmpdir.path().join(format!("extent_{}.dat", size));
            let extent_id = (1000 + size) as u64;
            let extent = Arc::new(Extent::create(&extent_path, extent_id).unwrap());
            let revision = 1i64;
            extent.has_lock(revision);
            
            b.iter(|| {
                // 只测量append_blocks的性能
                let blocks = create_test_data(size);
                let result = black_box(extent.append_blocks(blocks, false));
                assert!(result.is_ok());
            });
        });
    }

    group.finish();
}

fn bench_append_blocks_sync_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_blocks_sync_modes");
    let size = 4096;
    let blocks = create_test_data(size);

    group.throughput(Throughput::Bytes(size as u64));

    // NoSync mode
    group.bench_function("nosync", |b| {
        let tmpdir = TempDir::new().unwrap();
        let extent_path = tmpdir.path().join("nosync.dat");
        let extent = Arc::new(Extent::create(&extent_path, 12345).unwrap());
        let revision = 1i64;
        extent.has_lock(revision);

        b.iter(|| {
            let blocks_clone = blocks.clone();
            let result = black_box(extent.append_blocks(blocks_clone, false));
            assert!(result.is_ok());
        });
    });

    // WithSync mode
    group.bench_function("withsync", |b| {
        let tmpdir = TempDir::new().unwrap();
        let extent_path = tmpdir.path().join("withsync.dat");
        let extent = Arc::new(Extent::create(&extent_path, 12346).unwrap());
        let revision = 1i64;
        extent.has_lock(revision);

        b.iter(|| {
            let blocks_clone = blocks.clone();
            let result = black_box(extent.append_blocks(blocks_clone, true));
            assert!(result.is_ok());
        });
    });

    group.finish();
}

fn bench_append_blocks_concurrent(c: &mut Criterion) {
    use std::thread;
    
    let mut group = c.benchmark_group("append_blocks_concurrent");
    let size = 4096;
    
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("sequential", |b| {
        let tmpdir = TempDir::new().unwrap();
        let extent_path = tmpdir.path().join("sequential.dat");
        let extent = Arc::new(Extent::create(&extent_path, 20000).unwrap());
        let revision = 1i64;
        extent.has_lock(revision);

        b.iter(|| {
            let blocks = create_test_data(size);
            let result = black_box(extent.append_blocks(blocks, false));
            assert!(result.is_ok());
        });
    });

    // Multi-extent parallel writes (simulates real workload)
    group.bench_function("parallel_4_extents", |b| {
        let tmpdir = TempDir::new().unwrap();
        let extents: Vec<Arc<Extent>> = (0..4)
            .map(|i| {
                let path = tmpdir.path().join(format!("parallel_{}.dat", i));
                Extent::create(&path, 30000 + i as u64).unwrap()
            })
            .collect();
        
        // Lock all extents
        for extent in &extents {
            extent.has_lock(1i64);
        }

        b.iter(|| {
            let handles: Vec<_> = extents
                .iter()
                .map(|extent| {
                    let extent = extent.clone();
                    thread::spawn(move || {
                        let blocks = create_test_data(size);
                        extent.append_blocks(blocks, false).unwrap()
                    })
                })
                .collect();

            for handle in handles {
                black_box(handle.join().unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_append_blocks_sizes,
    bench_append_blocks_sync_modes,
    bench_append_blocks_concurrent
);
criterion_main!(benches);
