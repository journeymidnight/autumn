// Microbenchmarks for the IO engine write path.
// Tests BlockingIoEngine vs direct spawn_blocking pwrite to isolate channel overhead.
//
// Run: cargo bench -p autumn-io-engine

use std::os::unix::fs::FileExt;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use autumn_io_engine::{build_engine, Bytes, IoEngine, IoMode};

fn make_rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 1: BlockingIoEngine sequential writes (single file, increasing size)
// ─────────────────────────────────────────────────────────────────────────────
fn bench_blocking_io_sequential(c: &mut Criterion) {
    let rt = make_rt();
    let tmp = TempDir::new().unwrap();
    let engine = rt.block_on(async { build_engine(IoMode::Standard).unwrap() });
    let file = rt.block_on(async { engine.create(&tmp.path().join("bench.dat")).await.unwrap() });

    let mut group = c.benchmark_group("blocking_io_sequential");
    for size in [8 * 1024usize, 64 * 1024, 256 * 1024, 1024 * 1024] {
        let data = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}KB", size / 1024)),
            &size,
            |b, _| {
                let file = file.clone();
                let data = data.clone();
                let mut offset = 0u64;
                b.to_async(&rt).iter(|| {
                    let file = file.clone();
                    let data = data.clone();
                    let off = offset;
                    offset += size as u64;
                    async move {
                        file.write_at(off, Bytes::from(data)).await.unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 2: spawn_blocking + pwrite directly (no channel, baseline)
// ─────────────────────────────────────────────────────────────────────────────
fn bench_spawn_blocking_pwrite(c: &mut Criterion) {
    use std::fs::OpenOptions;
    let rt = make_rt();
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("bench_raw.dat");

    // Pre-open the file (keep fd open across iterations).
    let file = Arc::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap(),
    );

    let mut group = c.benchmark_group("spawn_blocking_pwrite");
    for size in [8 * 1024usize, 64 * 1024, 256 * 1024, 1024 * 1024] {
        let data = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}KB", size / 1024)),
            &size,
            |b, _| {
                let file = file.clone();
                let data = data.clone();
                let mut offset = 0u64;
                b.to_async(&rt).iter(|| {
                    let file = file.clone();
                    let data = data.clone();
                    let off = offset;
                    offset += size as u64;
                    async move {
                        tokio::task::spawn_blocking(move || {
                            file.write_at(&data, off).unwrap();
                        })
                        .await
                        .unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 3: 4 files in parallel via BlockingIoEngine (saturates workers)
// ─────────────────────────────────────────────────────────────────────────────
fn bench_blocking_io_parallel(c: &mut Criterion) {
    let rt = make_rt();
    let tmp = TempDir::new().unwrap();
    let engine = rt.block_on(async { build_engine(IoMode::Standard).unwrap() });

    let files: Vec<_> = rt.block_on(async {
        let mut v = Vec::new();
        for i in 0..4 {
            let f = engine
                .create(&tmp.path().join(format!("bench{i}.dat")))
                .await
                .unwrap();
            v.push(f);
        }
        v
    });

    const SIZE: usize = 256 * 1024;
    let data = vec![0xABu8; SIZE];

    let mut group = c.benchmark_group("blocking_io_parallel_4files");
    group.throughput(Throughput::Bytes((SIZE * 4) as u64));
    group.bench_function("256KB×4", |b| {
        let files = files.clone();
        let data = data.clone();
        let mut offsets = [0u64; 4];
        b.to_async(&rt).iter(|| {
            let files = files.clone();
            let data = data.clone();
            let offs = offsets;
            for o in offsets.iter_mut() {
                *o += SIZE as u64;
            }
            async move {
                let tasks: Vec<_> = files
                    .into_iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let d = data.clone();
                        let off = offs[i];
                        tokio::spawn(async move { f.write_at(off, Bytes::from(d)).await.unwrap() })
                    })
                    .collect();
                for t in tasks {
                    t.await.unwrap();
                }
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_blocking_io_sequential,
    bench_spawn_blocking_pwrite,
    bench_blocking_io_parallel,
);
criterion_main!(benches);
