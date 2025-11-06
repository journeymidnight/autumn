#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::Extent;
    use bytes::Bytes;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_extent_creation() {
        let temp_dir = TempDir::new().unwrap();
        let extent_path = temp_dir.path().join("test_extent.ext");
        
        let extent = Extent::create(&extent_path, 12345).unwrap();
        assert_eq!(extent.id(), 12345);
        assert!(!extent.is_sealed());
        assert_eq!(extent.commit_length(), 0);
    }

    #[tokio::test]
    async fn test_extent_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let extent_path = temp_dir.path().join("test_extent.ext");
        
        let extent = Extent::create(&extent_path, 12345).unwrap();
        
        // Test append
        let test_data = vec![
            Bytes::from("Hello, World!"),
            Bytes::from("This is a test block"),
        ];
        
        let (offsets, end) = extent.append_blocks(test_data.clone(), true).unwrap();
        assert_eq!(offsets.len(), 2);
        assert!(end > 0);
        assert_eq!(extent.commit_length(), end);
        
        // Test read
        let (blocks, read_offsets, read_end) = extent.read_blocks(0, 10, 1024 * 1024).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(read_offsets, offsets);
        assert_eq!(read_end, end);
        
        // Verify data
        assert_eq!(blocks[0], test_data[0]);
        assert_eq!(blocks[1], test_data[1]);
    }

    #[tokio::test]
    async fn test_extent_seal() {
        let temp_dir = TempDir::new().unwrap();
        let extent_path = temp_dir.path().join("test_extent.ext");
        
        let extent = Extent::create(&extent_path, 12345).unwrap();
        
        let test_data = vec![Bytes::from("Test data")];
        let (_, end) = extent.append_blocks(test_data, true).unwrap();
        
        // Seal the extent
        extent.seal(end).unwrap();
        assert!(extent.is_sealed());
        
        // Should not be able to append after sealing
        let more_data = vec![Bytes::from("More data")];
        let result = extent.append_blocks(more_data, true);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_extent_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let extent_path = temp_dir.path().join("test_extent.ext");
        
        let extent = Extent::create(&extent_path, 12345).unwrap();
        
        // Simulate recovery data
        let recovery_blocks = vec![
            Bytes::from("Recovery block 1"),
            Bytes::from("Recovery block 2"),
        ];
        
        extent.recovery_data(0, 100, recovery_blocks.clone()).unwrap();
        
        // Verify recovered data can be read
        let (blocks, _, _) = extent.read_blocks(0, 10, 1024 * 1024).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0], recovery_blocks[0]);
        assert_eq!(blocks[1], recovery_blocks[1]);
    }
}