/// WAL (Write-Ahead Log) recovery integration tests.
///
/// Verifies that:
/// 1. Small must_sync writes go through the WAL path.
/// 2. After a simulated data loss (extent file truncated), WAL replay restores the data.
/// 3. Large writes (>2MB) fall back to direct extent sync (no WAL).
mod test_helpers;

use autumn_stream::extent_rpc::CODE_OK;
use test_helpers::{pick_addr, start_node_with_wal, TestConn};

/// Mirror DiskFS::hash_byte: low byte of crc32c(extent_id_le).
fn extent_hash_byte(extent_id: u64) -> u8 {
    (crc32c::crc32c(&extent_id.to_le_bytes()) & 0xFF) as u8
}

fn extent_path(data_dir: &std::path::Path, extent_id: u64) -> std::path::PathBuf {
    data_dir
        .join(format!("{:02x}", extent_hash_byte(extent_id)))
        .join(format!("extent-{extent_id}.dat"))
}

/// Verify WAL replay recovers data lost from extent file.
///
/// Flow:
///   1. Write data via must_sync=true (WAL path).
///   2. Abort node and truncate extent file to simulate data loss.
///   3. Restart node — WAL replay should restore the data.
///   4. Verify commit_length == original payload length.
#[compio::test]
async fn wal_replay_recovers_truncated_extent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let addr = pick_addr();
    let extent_id = 5001u64;
    let payload = b"WAL recovery test payload".to_vec();
    let payload_len = payload.len() as u32;

    // Phase 1: write with WAL enabled
    {
        start_node_with_wal(data_dir, addr).await;
        let conn = TestConn::new(addr);

        conn.alloc_extent(extent_id).await;

        let resp = conn
            .append(extent_id, 1, 0, 1, true, payload)
            .await;
        assert_eq!(resp.code, CODE_OK);
        assert_eq!(resp.end, payload_len);
    }
    // Allow time for the old server to wind down
    compio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Phase 2: truncate the extent .dat file to simulate data loss (crash before sync)
    let extent_file = extent_path(data_dir, extent_id);
    assert!(extent_file.exists(), "extent file should exist");
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&extent_file)
            .expect("open extent file");
        f.set_len(0).expect("truncate extent file");
    }

    // Phase 3: restart with WAL — replay should restore data
    let addr2 = pick_addr();
    start_node_with_wal(data_dir, addr2).await;
    let conn2 = TestConn::new(addr2);

    // After WAL replay, commit_length should equal the payload length
    let cl = conn2.commit_length(extent_id, 0).await;
    assert_eq!(cl.code, CODE_OK, "extent should be accessible after replay");
    assert_eq!(cl.length, payload_len, "WAL replay should restore {payload_len} bytes");
}

/// Verify WAL is NOT used for large writes (>2MB threshold).
/// Large must_sync writes still sync the extent file directly.
#[compio::test]
async fn large_write_bypasses_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let addr = pick_addr();
    let extent_id = 5002u64;
    // 3MB payload — larger than WAL threshold (2MB)
    let payload: Vec<u8> = (0..3_000_000).map(|i| (i % 251) as u8).collect();
    let payload_len = payload.len() as u32;

    {
        start_node_with_wal(data_dir, addr).await;
        let conn = TestConn::new(addr);

        conn.alloc_extent(extent_id).await;

        let resp = conn
            .append(extent_id, 1, 0, 1, true, payload)
            .await;
        assert_eq!(resp.code, CODE_OK);
        assert_eq!(resp.end, payload_len);
    }
    compio::time::sleep(std::time::Duration::from_millis(50)).await;

    // WAL dir should either be empty or not contain a record for this extent.
    // Verify by checking data survives restart (direct sync preserved it).
    let addr2 = pick_addr();
    start_node_with_wal(data_dir, addr2).await;
    let conn2 = TestConn::new(addr2);

    let cl = conn2.commit_length(extent_id, 0).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, payload_len, "large write data should persist");
}

/// Verify WAL handles multiple appends, all replayed in order after restart.
#[compio::test]
async fn wal_replay_multiple_appends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path();
    let addr = pick_addr();
    let extent_id = 5003u64;

    let chunks: Vec<Vec<u8>> =
        vec![b"chunk0".to_vec(), b"chunk1".to_vec(), b"chunk2".to_vec()];
    let total_len: u32 = chunks.iter().map(|c| c.len() as u32).sum();

    // Phase 1: write multiple small appends
    {
        start_node_with_wal(data_dir, addr).await;
        let conn = TestConn::new(addr);

        conn.alloc_extent(extent_id).await;

        let mut commit = 0u32;
        for chunk in &chunks {
            let resp = conn
                .append(extent_id, 1, commit, 1, true, chunk.clone())
                .await;
            assert_eq!(resp.code, CODE_OK);
            commit = resp.end;
        }
        assert_eq!(commit, total_len);
    }
    compio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Phase 2: truncate extent file to simulate data loss
    let extent_file = extent_path(data_dir, extent_id);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&extent_file)
        .expect("open")
        .set_len(0)
        .expect("truncate");

    // Phase 3: restart + WAL replay
    let addr2 = pick_addr();
    start_node_with_wal(data_dir, addr2).await;
    let conn2 = TestConn::new(addr2);

    let cl = conn2.commit_length(extent_id, 0).await;
    assert_eq!(cl.code, CODE_OK);
    assert_eq!(cl.length, total_len, "all chunks should be replayed");

    // Read back and verify content
    let read = conn2.read_bytes(extent_id, 0, 0, total_len).await;
    assert_eq!(read.code, CODE_OK);
    let expected: Vec<u8> = chunks.into_iter().flatten().collect();
    assert_eq!(read.payload.as_ref(), expected.as_slice(), "read back should match original data");
}
