//! KV key encoding/decoding for the FUSE filesystem.
//!
//! Key layout uses a single-byte type prefix + big-endian u64 fields for natural sort order.
//!
//! | Prefix | Purpose       | Key format                                |
//! |--------|---------------|-------------------------------------------|
//! | 0x01   | Inode meta    | [0x01][ino: u64 BE]                       |
//! | 0x02   | Dir entry     | [0x02][parent_ino: u64 BE][name bytes]    |
//! | 0x03   | File chunk    | [0x03][ino: u64 BE][chunk_idx: u64 BE]    |
//! | 0x04   | FS superblock | [0x04][field bytes]                       |

const PREFIX_INODE: u8 = 0x01;
const PREFIX_DIRENT: u8 = 0x02;
const PREFIX_CHUNK: u8 = 0x03;
const PREFIX_SUPER: u8 = 0x04;

/// Encode inode metadata key: `[0x01][ino BE]`
pub fn inode_key(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(PREFIX_INODE);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

/// Decode inode number from an inode key.
pub fn parse_inode_key(key: &[u8]) -> Option<u64> {
    if key.len() == 9 && key[0] == PREFIX_INODE {
        Some(u64::from_be_bytes(key[1..9].try_into().unwrap()))
    } else {
        None
    }
}

/// Encode directory entry key: `[0x02][parent_ino BE][name]`
pub fn dirent_key(parent_ino: u64, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(9 + name.len());
    key.push(PREFIX_DIRENT);
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key.extend_from_slice(name);
    key
}

/// Encode directory entry prefix for Range scan: `[0x02][parent_ino BE]`
pub fn dirent_prefix(parent_ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(PREFIX_DIRENT);
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key
}

/// Parse directory entry key → (parent_ino, name bytes).
pub fn parse_dirent_key(key: &[u8]) -> Option<(u64, &[u8])> {
    if key.len() >= 9 && key[0] == PREFIX_DIRENT {
        let parent = u64::from_be_bytes(key[1..9].try_into().unwrap());
        let name = &key[9..];
        Some((parent, name))
    } else {
        None
    }
}

/// Encode file data chunk key: `[0x03][ino BE][chunk_idx BE]`
pub fn chunk_key(ino: u64, chunk_idx: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(PREFIX_CHUNK);
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(&chunk_idx.to_be_bytes());
    key
}

/// Parse chunk key → (ino, chunk_idx).
pub fn parse_chunk_key(key: &[u8]) -> Option<(u64, u64)> {
    if key.len() == 17 && key[0] == PREFIX_CHUNK {
        let ino = u64::from_be_bytes(key[1..9].try_into().unwrap());
        let chunk_idx = u64::from_be_bytes(key[9..17].try_into().unwrap());
        Some((ino, chunk_idx))
    } else {
        None
    }
}

/// Encode superblock field key: `[0x04][field]`
pub fn super_key(field: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + field.len());
    key.push(PREFIX_SUPER);
    key.extend_from_slice(field);
    key
}

/// Well-known superblock key for the next inode counter.
pub fn next_inode_key() -> Vec<u8> {
    super_key(b"next_inode")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inode_key_roundtrip() {
        let key = inode_key(42);
        assert_eq!(parse_inode_key(&key), Some(42));
    }

    #[test]
    fn dirent_key_roundtrip() {
        let key = dirent_key(1, b"hello.txt");
        let (parent, name) = parse_dirent_key(&key).unwrap();
        assert_eq!(parent, 1);
        assert_eq!(name, b"hello.txt");
    }

    #[test]
    fn chunk_key_roundtrip() {
        let key = chunk_key(100, 5);
        let (ino, idx) = parse_chunk_key(&key).unwrap();
        assert_eq!(ino, 100);
        assert_eq!(idx, 5);
    }

    #[test]
    fn dirent_keys_sort_by_parent_then_name() {
        let k1 = dirent_key(1, b"aaa");
        let k2 = dirent_key(1, b"bbb");
        let k3 = dirent_key(2, b"aaa");
        assert!(k1 < k2, "same parent, name a < b");
        assert!(k2 < k3, "parent 1 < parent 2");
    }

    #[test]
    fn chunk_keys_sort_by_ino_then_index() {
        let k1 = chunk_key(10, 0);
        let k2 = chunk_key(10, 1);
        let k3 = chunk_key(11, 0);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn type_prefixes_isolate_namespaces() {
        let ik = inode_key(1);
        let dk = dirent_key(1, b"x");
        let ck = chunk_key(1, 0);
        let sk = next_inode_key();
        assert!(ik < dk, "inode prefix 0x01 < dirent prefix 0x02");
        assert!(dk < ck, "dirent prefix 0x02 < chunk prefix 0x03");
        assert!(ck < sk, "chunk prefix 0x03 < super prefix 0x04");
    }
}
