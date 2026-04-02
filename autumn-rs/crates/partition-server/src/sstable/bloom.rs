/// Simple bloom filter using double hashing with xxh3.
///
/// Uses the standard double-hashing construction:
///   hash_i(x) = (h1(x) + i * h2(x)) mod m
/// where h1 = xxh3_64(x) and h2 = xxh3_64_with_seed(x, 0xC0FFEE).
///
/// For the user key, the 8-byte MVCC timestamp suffix is stripped before hashing
/// (matching Go's `farm.Fingerprint64(y.ParseKey(key))`).
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

const SEED2: u64 = 0xC0FF_EE42_DEAD_BEEF;

/// Computes the two base hashes for a user key.
fn hash_pair(user_key: &[u8]) -> (u64, u64) {
    let h1 = xxh3_64(user_key);
    let h2 = xxh3_64_with_seed(user_key, SEED2);
    (h1, h2)
}

/// Builder for a bloom filter.
pub struct BloomFilterBuilder {
    num_bits: usize,
    num_hashes: u32,
    bits: Vec<u8>,
}

impl BloomFilterBuilder {
    /// Create a new builder for `expected_keys` keys with `fpr` false-positive rate.
    pub fn new(expected_keys: usize, fpr: f64) -> Self {
        let n = expected_keys.max(1) as f64;
        let fpr = fpr.clamp(1e-6, 0.5);
        // m = -n * ln(fpr) / (ln(2))^2
        let m = (-n * fpr.ln() / (2f64.ln().powi(2))).ceil() as usize;
        // k = (m/n) * ln(2)
        let k = ((m as f64 / n) * 2f64.ln()).round() as u32;
        let num_bits = m.max(8);
        let num_bytes = (num_bits + 7) / 8;
        BloomFilterBuilder {
            num_bits,
            num_hashes: k.max(1),
            bits: vec![0u8; num_bytes],
        }
    }

    /// Add `user_key` (timestamp suffix already stripped) to the filter.
    pub fn add_user_key(&mut self, user_key: &[u8]) {
        let (h1, h2) = hash_pair(user_key);
        let m = self.num_bits as u64;
        for i in 0..self.num_hashes as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2))) % m;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// Build and return the finished filter.
    pub fn finish(self) -> BloomFilter {
        BloomFilter {
            bits: self.bits,
            num_bits: self.num_bits,
            num_hashes: self.num_hashes,
        }
    }
}

/// Immutable bloom filter.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Returns `true` if `user_key` **may** be in the set.
    /// Returns `false` if the key is **definitely not** in the set.
    pub fn may_contain(&self, user_key: &[u8]) -> bool {
        if self.num_bits == 0 {
            return true; // empty filter: assume present
        }
        let (h1, h2) = hash_pair(user_key);
        let m = self.num_bits as u64;
        for i in 0..self.num_hashes as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2))) % m;
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Encode to bytes: [num_bits: u32 LE][num_hashes: u32 LE][bits...]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len());
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Decode from bytes produced by `encode()`.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let bits = data[8..].to_vec();
        Some(BloomFilter {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000u64).map(|i| i.to_le_bytes().to_vec()).collect();
        let mut b = BloomFilterBuilder::new(keys.len(), 0.01);
        for k in &keys {
            b.add_user_key(k);
        }
        let f = b.finish();
        for k in &keys {
            assert!(f.may_contain(k), "false negative for key {k:?}");
        }
    }

    #[test]
    fn round_trip_encode_decode() {
        let mut b = BloomFilterBuilder::new(100, 0.01);
        for i in 0u64..100 {
            b.add_user_key(&i.to_le_bytes());
        }
        let f = b.finish();
        let encoded = f.encode();
        let f2 = BloomFilter::decode(&encoded).expect("decode");
        for i in 0u64..100 {
            assert!(f2.may_contain(&i.to_le_bytes()));
        }
    }
}
