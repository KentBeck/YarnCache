//! Tests for the transaction log sequence number functionality

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;
    use temp_dir::TempDir;

    use crate::transaction_log::{Operation, TransactionLog};
    use crate::types::{Node, NodeId, TypeId};
    use crate::Result;

    #[test]
    fn test_sequence_file() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("test.log");

        // Create a new transaction log
        let log = TransactionLog::new(&log_path)?;

        // The sequence file should be created with sequence 0
        let sequence_path = log_path.with_extension("log.seq");
        assert!(Path::new(&sequence_path).exists());

        // Read the sequence file and verify it contains 0
        let mut file = File::open(&sequence_path)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let sequence = u64::from_le_bytes(buf);
        assert_eq!(sequence, 0);

        // Add some entries to the log
        let node1 = Node::new(NodeId(1), TypeId(2), vec![1, 2, 3]);
        let node2 = Node::new(NodeId(2), TypeId(2), vec![4, 5, 6]);
        let node3 = Node::new(NodeId(3), TypeId(2), vec![7, 8, 9]);

        log.append(Operation::AddNode(node1))?;
        log.append(Operation::AddNode(node2))?;
        log.append(Operation::AddNode(node3))?;

        // The sequence file should now contain 3
        let mut file = File::open(&sequence_path)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let sequence = u64::from_le_bytes(buf);
        assert_eq!(sequence, 3);

        // Create a new transaction log with the same path
        let log2 = TransactionLog::new(&log_path)?;

        // Add another entry
        let node4 = Node::new(NodeId(4), TypeId(2), vec![10, 11, 12]);
        log2.append(Operation::AddNode(node4))?;

        // The sequence file should now contain 4
        let mut file = File::open(&sequence_path)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let sequence = u64::from_le_bytes(buf);
        assert_eq!(sequence, 4);

        // Truncate the log
        log2.truncate()?;

        // The sequence file should now contain 0
        let mut file = File::open(&sequence_path)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let sequence = u64::from_le_bytes(buf);
        assert_eq!(sequence, 0);

        Ok(())
    }

    #[test]
    fn test_sequence_file_recovery() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("recovery.log");

        // Create a new transaction log
        let log = TransactionLog::new(&log_path)?;

        // Add some entries to the log
        let node1 = Node::new(NodeId(1), TypeId(2), vec![1, 2, 3]);
        let node2 = Node::new(NodeId(2), TypeId(2), vec![4, 5, 6]);

        log.append(Operation::AddNode(node1))?;
        log.append(Operation::AddNode(node2))?;

        // Delete the sequence file to simulate corruption
        let sequence_path = log_path.with_extension("log.seq");
        std::fs::remove_file(&sequence_path)?;

        // Create a new transaction log with the same path
        // It should recover the sequence number by scanning the log
        let log2 = TransactionLog::new(&log_path)?;

        // The sequence file should be recreated with sequence 2
        assert!(Path::new(&sequence_path).exists());

        // Read the sequence file and verify it contains 2
        let mut file = File::open(&sequence_path)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let sequence = u64::from_le_bytes(buf);
        assert_eq!(sequence, 2);

        // Add another entry
        let node3 = Node::new(NodeId(3), TypeId(2), vec![7, 8, 9]);
        log2.append(Operation::AddNode(node3))?;

        // The sequence file should now contain 3
        let mut file = File::open(&sequence_path)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let sequence = u64::from_le_bytes(buf);
        assert_eq!(sequence, 3);

        Ok(())
    }
}
