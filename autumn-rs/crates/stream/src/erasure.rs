use anyhow::{anyhow, Result};
use reed_solomon_erasure::galois_8::ReedSolomon;

/// Per-shard byte size for a given payload length and number of data shards.
///
/// Matches Go rs_codec.go formula:
///   perShard := (size + dataShards + 4 - 1) / dataShards
///   if perShard < 4 { perShard = 4 }
///
/// The `+ dataShards + 4 - 1` ensures at least 4 bytes of padding remain in the
/// last data shard to store the original payload length trailer.
pub fn shard_size(payload_len: usize, data_shards: usize) -> usize {
    let per_shard = (payload_len + data_shards + 4 - 1) / data_shards;
    per_shard.max(4)
}

/// Encode a payload into `data_shards + parity_shards` equal-length byte slices.
///
/// Encoding format (matches Go ReedSolomon.Encode):
///   - Payload bytes are distributed across the first `data_shards` slices.
///   - The last 4 bytes of `shards[data_shards-1]` store `payload_len` as big-endian u32.
///   - Parity shards are computed by the Reed-Solomon encoder.
///
/// All returned shards have length `shard_size(payload.len(), data_shards)`.
pub fn ec_encode(
    payload: &[u8],
    data_shards: usize,
    parity_shards: usize,
) -> Result<Vec<Vec<u8>>> {
    if data_shards == 0 {
        return Err(anyhow!("data_shards must be > 0"));
    }
    if parity_shards == 0 {
        return Err(anyhow!("parity_shards must be > 0 for EC encode"));
    }

    let size = payload.len();
    let per_shard = shard_size(size, data_shards);
    let total_shards = data_shards + parity_shards;

    debug_assert!(
        per_shard * data_shards >= size + 4,
        "not enough space for length trailer: per_shard={} data_shards={} size={}",
        per_shard,
        data_shards,
        size,
    );

    // Allocate all shards zero-filled.
    let mut shards: Vec<Vec<u8>> = (0..total_shards)
        .map(|_| vec![0u8; per_shard])
        .collect();

    // Copy payload across data shards.
    let mut remaining = size;
    for i in 0..data_shards {
        if remaining == 0 {
            break;
        }
        let start = i * per_shard;
        let n = remaining.min(per_shard);
        shards[i][..n].copy_from_slice(&payload[start..start + n]);
        remaining -= n;
    }

    // Write original payload length as big-endian u32 into the last 4 bytes
    // of the last data shard.
    let trailer_pos = per_shard - 4;
    let len_bytes = (size as u32).to_be_bytes();
    shards[data_shards - 1][trailer_pos..].copy_from_slice(&len_bytes);

    // Compute parity shards.
    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;
    rs.encode(&mut shards)
        .map_err(|e| anyhow!("RS encode failed: {e}"))?;

    Ok(shards)
}

/// Decode original payload from `data_shards + parity_shards` shards.
///
/// Up to `parity_shards` elements of `shards` may be `None` (missing/unavailable).
/// Requires at least `data_shards` non-None elements.
///
/// Matches Go ReedSolomon.Decode behavior including the length trailer.
pub fn ec_decode(
    mut shards: Vec<Option<Vec<u8>>>,
    data_shards: usize,
    parity_shards: usize,
) -> Result<Vec<u8>> {
    if shards.len() != data_shards + parity_shards {
        return Err(anyhow!(
            "shards len {} != data+parity {}",
            shards.len(),
            data_shards + parity_shards
        ));
    }

    let per_shard = shards
        .iter()
        .find_map(|s| s.as_ref().map(|v| v.len()))
        .ok_or_else(|| anyhow!("all shards are None"))?;

    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;

    // Reconstruct only data shards (more efficient than reconstructing parity too).
    rs.reconstruct_data(&mut shards)
        .map_err(|e| anyhow!("RS reconstruct_data failed: {e}"))?;

    // Read original size from last 4 bytes of last data shard.
    let last_data = shards[data_shards - 1]
        .as_ref()
        .ok_or_else(|| anyhow!("last data shard still missing after reconstruct"))?;
    if last_data.len() < 4 {
        return Err(anyhow!("last data shard too short to contain length trailer"));
    }
    let trailer_pos = per_shard - 4;
    let original_size =
        u32::from_be_bytes(last_data[trailer_pos..trailer_pos + 4].try_into().unwrap()) as usize;

    // Concatenate data shards and truncate to original size.
    let mut out = Vec::with_capacity(original_size);
    let mut remaining = original_size;
    for i in 0..data_shards {
        if remaining == 0 {
            break;
        }
        let shard = shards[i]
            .as_ref()
            .ok_or_else(|| anyhow!("data shard {} missing after reconstruct", i))?;
        let n = remaining.min(per_shard);
        out.extend_from_slice(&shard[..n]);
        remaining -= n;
    }
    Ok(out)
}

/// Reconstruct a single missing shard at `missing_index`.
///
/// Used for node recovery: reads shards from `data_shards` healthy peers and
/// reconstructs the exact shard bytes that the failed node should hold.
/// The returned bytes should be written verbatim to the recovering node's extent file.
pub fn ec_reconstruct_shard(
    mut shards: Vec<Option<Vec<u8>>>,
    data_shards: usize,
    parity_shards: usize,
    missing_index: usize,
) -> Result<Vec<u8>> {
    if shards.len() != data_shards + parity_shards {
        return Err(anyhow!(
            "shards len {} != data+parity {}",
            shards.len(),
            data_shards + parity_shards
        ));
    }
    if missing_index >= shards.len() {
        return Err(anyhow!(
            "missing_index {} out of range {}",
            missing_index,
            shards.len()
        ));
    }
    if shards[missing_index].is_some() {
        return Err(anyhow!(
            "shards[{}] is not None — expected missing shard slot",
            missing_index
        ));
    }

    let rs = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| anyhow!("ReedSolomon::new failed: {e}"))?;

    // Full reconstruct (data + parity) to rebuild whichever shard is missing.
    rs.reconstruct(&mut shards)
        .map_err(|e| anyhow!("RS reconstruct failed: {e}"))?;

    shards[missing_index]
        .take()
        .ok_or_else(|| anyhow!("shard {} still None after reconstruct", missing_index))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(payload: &[u8], data_shards: usize, parity_shards: usize) {
        let shards = ec_encode(payload, data_shards, parity_shards).unwrap();
        assert_eq!(shards.len(), data_shards + parity_shards);
        let shard_len = shards[0].len();
        for s in &shards {
            assert_eq!(s.len(), shard_len);
        }
        let decoded =
            ec_decode(shards.into_iter().map(Some).collect(), data_shards, parity_shards).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_encode_decode_roundtrip_various_sizes() {
        for &size in &[1usize, 3, 7, 8, 100, 1024, 4096, 65536, 1 << 20] {
            let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            roundtrip(&payload, 2, 1);
            roundtrip(&payload, 3, 1);
            roundtrip(&payload, 3, 2);
        }
    }

    #[test]
    fn test_encode_decode_6_3_config() {
        let payload: Vec<u8> = (0..8 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        roundtrip(&payload, 6, 3);
    }

    #[test]
    fn test_decode_with_missing_data_shard() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 3, 2).unwrap();
        // Remove shard 1 (a data shard).
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[1] = None;
        let decoded = ec_decode(shards_opt, 3, 2).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_decode_with_missing_parity_shard() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[2] = None; // Remove parity shard.
        let decoded = ec_decode(shards_opt, 2, 1).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_decode_max_missing_shards() {
        // With 3 parity shards, can lose any 3.
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 6, 3).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // Remove shards 1, 3, 5 (2 data, 1 parity).
        shards_opt[1] = None;
        shards_opt[3] = None;
        shards_opt[5] = None;
        let decoded = ec_decode(shards_opt, 6, 3).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_reconstruct_shard_data() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let original_shard0 = shards[0].clone();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[0] = None; // Simulate shard 0 being missing.
        let reconstructed = ec_reconstruct_shard(shards_opt, 2, 1, 0).unwrap();
        assert_eq!(reconstructed, original_shard0);
    }

    #[test]
    fn test_reconstruct_shard_parity() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let original_parity = shards[2].clone();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[2] = None; // Simulate parity shard missing.
        let reconstructed = ec_reconstruct_shard(shards_opt, 2, 1, 2).unwrap();
        assert_eq!(reconstructed, original_parity);
    }

    #[test]
    fn test_shard_size_formula() {
        // Verify formula matches Go: (size + dataShards + 4 - 1) / dataShards, min 4.
        // shard_size(0, 2)  = (0+2+4-1)/2 = 5/2 = 2, but min 4 → 4
        assert_eq!(shard_size(0, 2), 4);
        // shard_size(1, 2)  = (1+2+4-1)/2 = 6/2 = 3, but min 4 → 4
        assert_eq!(shard_size(1, 2), 4);
        // shard_size(8, 2)  = (8+2+4-1)/2 = 13/2 = 6; leftSpace=6*2-8=4 ≥ 4 ✓
        assert_eq!(shard_size(8, 2), 6);
        // shard_size(100, 4) = (100+4+4-1)/4 = 107/4 = 26; leftSpace=26*4-100=4 ≥ 4 ✓
        assert_eq!(shard_size(100, 4), 26);
        // shard_size(1024, 6) = (1024+6+4-1)/6 = 1033/6 = 172; leftSpace=172*6-1024=8 ≥ 4 ✓
        assert_eq!(shard_size(1024, 6), 172);
    }

    #[test]
    fn test_length_trailer_position() {
        let payload = b"hello world"; // 11 bytes
        let shards = ec_encode(payload, 2, 1).unwrap();
        let per_shard = shards[0].len();
        // Trailer is last 4 bytes of last data shard (index 1).
        let trailer = &shards[1][per_shard - 4..];
        let stored_len = u32::from_be_bytes(trailer.try_into().unwrap());
        assert_eq!(stored_len as usize, payload.len());
    }

    #[test]
    fn test_shard_size_all_equal() {
        let payload: Vec<u8> = (0..999).map(|i| i as u8).collect();
        let shards = ec_encode(&payload, 3, 2).unwrap();
        let first_len = shards[0].len();
        for s in &shards {
            assert_eq!(s.len(), first_len, "all shards must have equal length");
        }
    }

    /// Verify that data shards contain raw payload slices — the fast-path
    /// assumption that sub-range reads from a data shard return original bytes.
    #[test]
    fn test_data_shards_contain_raw_payload() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let per_shard = shards[0].len();

        // shard[0] should contain payload[0..per_shard] (minus trailer area)
        assert_eq!(&shards[0][..per_shard.min(payload.len())], &payload[..per_shard.min(payload.len())]);

        // Reading a sub-range from shard[0] gives the original payload bytes
        assert_eq!(&shards[0][100..200], &payload[100..200]);

        // shard[1] starts at payload[per_shard..]
        let remaining = payload.len() - per_shard;
        assert_eq!(&shards[1][..remaining], &payload[per_shard..per_shard + remaining]);
    }

    /// Verify that partial shards cannot be decoded — this is the bug that
    /// ec_read_from_extent had (passing offset/length to all shards).
    #[test]
    fn test_partial_shards_cannot_decode() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let per_shard = shards[0].len();

        // Take a sub-range from each shard (simulating the old buggy behavior)
        let partial: Vec<Option<Vec<u8>>> = shards
            .iter()
            .map(|s| Some(s[100..200].to_vec()))
            .collect();

        // This should fail because partial shards have wrong length for RS decode
        let result = ec_decode(partial, 2, 1);
        // The decode itself may "succeed" but return garbage (wrong length trailer).
        // Either way, the output won't match the expected payload sub-range.
        if let Ok(decoded) = result {
            // If it somehow decodes, the result is garbage — not payload[100..200]
            assert_ne!(decoded, payload[100..200].to_vec(),
                "partial shard decode should NOT return correct payload sub-range");
        }
        // If it errors, that's also correct — partial shards can't be decoded.
    }

    /// 2+1 EC: missing one data shard, decode from remaining data + parity.
    #[test]
    fn test_2_plus_1_missing_data_shard_0() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[0] = None; // data shard 0 gone
        let decoded = ec_decode(shards_opt, 2, 1).unwrap();
        assert_eq!(decoded, payload, "should recover from 1 data + 1 parity");
    }

    /// 2+1 EC: missing data shard 1, decode from shard 0 + parity.
    #[test]
    fn test_2_plus_1_missing_data_shard_1() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let shards = ec_encode(&payload, 2, 1).unwrap();
        let mut shards_opt: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shards_opt[1] = None; // data shard 1 gone
        let decoded = ec_decode(shards_opt, 2, 1).unwrap();
        assert_eq!(decoded, payload, "should recover from 1 data + 1 parity");
    }
}
