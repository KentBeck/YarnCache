//! Tests for page-based storage of nodes and arcs
//!
//! This test verifies that nodes and arcs are properly stored on disk pages
//! and can be loaded on server start without relying on the transaction log.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use temp_dir::TempDir;
    use tokio::runtime::Runtime;

    use crate::server::ServerConfig;
    use crate::storage::{DEFAULT_PAGE_SIZE, StorageManager, Page, PageType};
    use crate::types::{Node, NodeId, TypeId, Timestamp};
    use crate::{Server, YarnCacheApi};

    // A simplified storage manager for testing
    struct SimpleStorageManager {
        db_path: std::path::PathBuf,
        nodes: std::collections::HashMap<u64, Node>,
    }

    impl SimpleStorageManager {
        fn new(db_path: std::path::PathBuf) -> Self {
            Self {
                db_path,
                nodes: std::collections::HashMap::new(),
            }
        }

        fn store_node(&mut self, node: &Node) -> std::io::Result<()> {
            // Store in memory
            self.nodes.insert(node.id.0, node.clone());

            // Store on disk
            let node_bytes = bincode::serialize(node).unwrap();

            use std::fs::OpenOptions;
            use std::io::Write;

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.db_path)?;

            // Write the number of nodes
            let num_nodes = 1u32;
            file.write_all(&num_nodes.to_le_bytes())?;

            // Write the node ID and length
            file.write_all(&node.id.0.to_le_bytes())?;
            file.write_all(&(node_bytes.len() as u32).to_le_bytes())?;

            // Write the node data
            file.write_all(&node_bytes)?;
            file.flush()?;

            Ok(())
        }

        fn get_node(&self, id: NodeId) -> std::io::Result<Option<Node>> {
            // First check in memory
            if let Some(node) = self.nodes.get(&id.0) {
                return Ok(Some(node.clone()));
            }

            // If not in memory, try to load from disk
            use std::fs::File;
            use std::io::Read;

            let file = match File::open(&self.db_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            };

            let mut reader = std::io::BufReader::new(file);

            // Read the number of nodes
            let mut num_nodes_bytes = [0u8; 4];
            if reader.read_exact(&mut num_nodes_bytes).is_err() {
                return Ok(None);
            }
            let num_nodes = u32::from_le_bytes(num_nodes_bytes);

            // Read each node
            for _ in 0..num_nodes {
                // Read the node ID
                let mut id_bytes = [0u8; 8];
                reader.read_exact(&mut id_bytes)?;
                let node_id = u64::from_le_bytes(id_bytes);

                // Read the node data length
                let mut len_bytes = [0u8; 4];
                reader.read_exact(&mut len_bytes)?;
                let data_len = u32::from_le_bytes(len_bytes) as usize;

                // Read the node data
                let mut node_data = vec![0; data_len];
                reader.read_exact(&mut node_data)?;

                // If this is the node we're looking for, deserialize and return it
                if node_id == id.0 {
                    let node: Node = bincode::deserialize(&node_data).unwrap();
                    return Ok(Some(node));
                }

                // Otherwise, skip to the next node
            }

            Ok(None)
        }

        fn shutdown(&mut self) -> std::io::Result<()> {
            // Nothing to do here since we don't keep any resources open
            Ok(())
        }
    }

    #[test]
    fn test_simple_storage_manager() {
        // This test uses our simplified storage manager
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_simple_storage_manager...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("simple_storage_test.db");

            // Create a node
            let id = NodeId(42);
            let type_id = TypeId(100);
            let timestamp = Timestamp(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64);
            let data = vec![1, 2, 3, 4, 5];
            let node = Node {
                id,
                type_id,
                timestamp,
                data: data.clone(),
            };

            // First phase: Create a storage manager, store a node, and shut it down
            {
                println!("Phase 1: Creating first storage manager...");
                let mut storage = SimpleStorageManager::new(db_path.clone());

                // Store the node
                println!("Storing node...");
                storage.store_node(&node).unwrap();

                // Verify the node exists in memory
                println!("Verifying node exists in memory...");
                let retrieved_node = storage.get_node(id).unwrap().unwrap();
                assert_eq!(retrieved_node.data, data);

                // Explicitly shutdown the storage manager
                println!("Shutting down first storage manager...");
                storage.shutdown().unwrap();

                // Let the storage manager be dropped
                println!("First storage manager will be dropped.");
            }

            // Make sure the first storage manager is fully dropped
            println!("First storage manager has been dropped.");

            // Second phase: Create a new storage manager and verify the node exists
            {
                println!("Phase 2: Creating second storage manager...");
                let storage2 = SimpleStorageManager::new(db_path.clone());

                // Verify the node exists in the new storage manager
                println!("Verifying node exists in new storage manager...");
                let loaded_node = storage2.get_node(id).unwrap();
                assert!(loaded_node.is_some(), "Node should be loaded from disk");
                assert_eq!(loaded_node.unwrap().data, data);

                println!("Second storage manager will be dropped.");
            }

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_storage_manager_with_separate_instances() {
        // This test creates two completely separate instances of StorageManager
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_storage_manager_with_separate_instances...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("separate_instances_test.db");

            // Create a node
            let id = NodeId(42);
            let type_id = TypeId(100);
            let timestamp = Timestamp(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64);
            let data = vec![1, 2, 3, 4, 5];
            let node = Node {
                id,
                type_id,
                timestamp,
                data: data.clone(),
            };

            // First phase: Create a storage manager, store a node, and shut it down
            {
                println!("Phase 1: Creating first storage manager...");
                let storage = StorageManager::new(
                    db_path.clone(),
                    DEFAULT_PAGE_SIZE,
                    NonZeroUsize::new(10).unwrap(),
                    None, // Unlimited for tests
                    1000, // 1 second flush interval
                ).unwrap();

                // Store the node
                println!("Storing node...");
                storage.store_node(&node).unwrap();

                // Verify the node exists in memory
                println!("Verifying node exists in memory...");
                let retrieved_node = storage.get_node(id).unwrap().unwrap();
                assert_eq!(retrieved_node.data, data);

                // Explicitly shutdown the storage manager
                println!("Shutting down first storage manager...");
                storage.shutdown().unwrap();

                // Let the storage manager be dropped
                println!("First storage manager will be dropped.");
            }

            // Make sure the first storage manager is fully dropped
            println!("First storage manager has been dropped.");

            // Second phase: Create a new storage manager and verify the node exists
            {
                // Wait a moment to ensure all resources are released
                std::thread::sleep(std::time::Duration::from_millis(100));

                println!("Phase 2: Creating second storage manager...");
                let storage2 = StorageManager::new(
                    db_path.clone(),
                    DEFAULT_PAGE_SIZE,
                    NonZeroUsize::new(10).unwrap(),
                    None, // Unlimited for tests
                    1000, // 1 second flush interval
                ).unwrap();

                // Verify the node exists in the new storage manager
                println!("Verifying node exists in new storage manager...");
                let loaded_node = storage2.get_node(id).unwrap();
                assert!(loaded_node.is_some(), "Node should be loaded from disk");
                assert_eq!(loaded_node.unwrap().data, data);

                // Explicitly shutdown the second storage manager
                println!("Shutting down second storage manager...");
                storage2.shutdown().unwrap();

                println!("Second storage manager will be dropped.");
            }

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_simplified_storage() {
        // This test uses a simplified storage approach without complex locking
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_simplified_storage...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("simplified_storage_test.db");

            // Create a node
            let id = NodeId(42);
            let type_id = TypeId(100);
            let timestamp = Timestamp(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64);
            let data = vec![1, 2, 3, 4, 5];
            let node = Node {
                id,
                type_id,
                timestamp,
                data: data.clone(),
            };

            // Serialize the node
            println!("Serializing node...");
            let node_bytes = bincode::serialize(&node).unwrap();

            // Write the node to a file
            println!("Writing node to file...");
            {
                use std::fs::File;
                use std::io::Write;

                let mut file = File::create(&db_path).unwrap();

                // Write the node ID and length as a header
                let header = format!("{},{}", node.id.0, node_bytes.len());
                file.write_all(header.as_bytes()).unwrap();
                file.write_all(b"\n").unwrap(); // Newline separator

                // Write the node data
                file.write_all(&node_bytes).unwrap();
                file.flush().unwrap();
                println!("Node written to file.");
            }

            // Read the node from the file
            println!("Reading node from file...");
            {
                use std::fs::File;
                use std::io::{BufRead, BufReader, Read};

                let file = File::open(&db_path).unwrap();
                let mut reader = BufReader::new(file);

                // Read the header
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let header = header.trim();
                let parts: Vec<&str> = header.split(',').collect();
                assert_eq!(parts.len(), 2, "Header should have ID and length");

                let stored_id = parts[0].parse::<u64>().unwrap();
                let data_len = parts[1].parse::<usize>().unwrap();

                assert_eq!(stored_id, node.id.0, "Stored ID should match original ID");

                // Read the node data
                let mut node_data = vec![0; data_len];
                reader.read_exact(&mut node_data).unwrap();

                // Deserialize the node
                let loaded_node: Node = bincode::deserialize(&node_data).unwrap();

                assert_eq!(loaded_node.id, node.id, "Loaded node ID should match original");
                assert_eq!(loaded_node.data, node.data, "Loaded node data should match original");

                println!("Node read successfully: ID={}, data={:?}", loaded_node.id.0, loaded_node.data);
            }

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_basic_file_io() {
        // This test uses standard Rust file I/O operations to verify that basic file operations work correctly
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_basic_file_io...");

            // Create a temporary directory for the file
            let temp_dir = TempDir::new().unwrap();
            let file_path = temp_dir.path().join("basic_file_test.txt");

            // Write data to the file
            println!("Writing data to file...");
            {
                use std::fs::File;
                use std::io::Write;

                let mut file = File::create(&file_path).unwrap();
                file.write_all(b"Hello, world!").unwrap();
                file.flush().unwrap();
                // File is closed when it goes out of scope
                println!("File written and closed.");
            }

            // Read data from the file
            println!("Reading data from file...");
            {
                use std::fs::File;
                use std::io::Read;

                let mut file = File::open(&file_path).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();
                assert_eq!(contents, "Hello, world!");
                println!("File read successfully: {}", contents);
            }

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_storage_manager_with_explicit_shutdown() {
        // This test explicitly shuts down the storage manager before creating a new one
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_storage_manager_with_explicit_shutdown...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("explicit_shutdown_test.db");

            // Create a storage manager
            println!("Creating first storage manager...");
            let storage = StorageManager::new(
                db_path.clone(),
                DEFAULT_PAGE_SIZE,
                NonZeroUsize::new(10).unwrap(),
                None, // Unlimited for tests
                1000, // 1 second flush interval
            ).unwrap();

            // Create a node
            let id = NodeId(42);
            let type_id = TypeId(100);
            let timestamp = Timestamp(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64);
            let data = vec![1, 2, 3, 4, 5];
            let node = Node {
                id,
                type_id,
                timestamp,
                data: data.clone(),
            };

            // Store the node
            println!("Storing node...");
            storage.store_node(&node).unwrap();

            // Verify the node exists in memory
            println!("Verifying node exists in memory...");
            let retrieved_node = storage.get_node(id).unwrap().unwrap();
            assert_eq!(retrieved_node.data, data);

            // Explicitly shutdown the storage manager
            println!("Shutting down first storage manager...");
            storage.shutdown().unwrap();

            // Drop the storage manager to release all resources
            drop(storage);
            println!("First storage manager dropped.");

            // Create a new storage manager with the same database file
            println!("Creating second storage manager...");
            let storage2 = StorageManager::new(
                db_path.clone(),
                DEFAULT_PAGE_SIZE,
                NonZeroUsize::new(10).unwrap(),
                None, // Unlimited for tests
                1000, // 1 second flush interval
            ).unwrap();

            // Verify the node exists in the new storage manager
            println!("Verifying node exists in new storage manager...");
            let loaded_node = storage2.get_node(id).unwrap();
            assert!(loaded_node.is_some(), "Node should be loaded from disk");
            assert_eq!(loaded_node.unwrap().data, data);

            // Explicitly shutdown the second storage manager
            println!("Shutting down second storage manager...");
            storage2.shutdown().unwrap();

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_storage_manager_node_persistence() {
        // This test directly tests the StorageManager's ability to store and retrieve nodes
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_storage_manager_node_persistence...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("node_persistence_test.db");

            // Create a storage manager
            println!("Creating storage manager...");
            let storage = StorageManager::new(
                db_path.clone(),
                DEFAULT_PAGE_SIZE,
                NonZeroUsize::new(10).unwrap(),
                None, // Unlimited for tests
                1000, // 1 second flush interval
            ).unwrap();

            // Create a node
            let id = NodeId(42);
            let type_id = TypeId(100);
            let timestamp = Timestamp(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64);
            let data = vec![1, 2, 3, 4, 5];
            let node = Node {
                id,
                type_id,
                timestamp,
                data: data.clone(),
            };

            // Store the node
            println!("Storing node...");
            storage.store_node(&node).unwrap();

            // Verify the node exists in memory
            println!("Verifying node exists in memory...");
            let retrieved_node = storage.get_node(id).unwrap().unwrap();
            assert_eq!(retrieved_node.data, data);

            // Manually write the page to disk
            println!("Writing page to disk...");
            storage.flush_all().unwrap();

            // Drop the storage manager to close the file
            drop(storage);

            // Create a new storage manager with the same database file
            println!("Creating new storage manager...");
            let storage2 = StorageManager::new(
                db_path.clone(),
                DEFAULT_PAGE_SIZE,
                NonZeroUsize::new(10).unwrap(),
                None, // Unlimited for tests
                1000, // 1 second flush interval
            ).unwrap();

            // Verify the node exists in the new storage manager
            println!("Verifying node exists in new storage manager...");
            let retrieved_node = storage2.get_node(id).unwrap();
            assert!(retrieved_node.is_some(), "Node should be loaded from disk");
            assert_eq!(retrieved_node.unwrap().data, data);

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_direct_page_storage() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_direct_page_storage...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("direct_page_test.db");

            // Create a storage manager directly
            println!("Creating storage manager...");
            let storage = StorageManager::new(
                db_path.clone(),
                DEFAULT_PAGE_SIZE,
                NonZeroUsize::new(10).unwrap(),
                None, // Unlimited for tests
                1000, // 1 second flush interval
            ).unwrap();

            // Add a node directly to the storage
            println!("Adding a node...");
            let id = NodeId(42);
            let type_id = TypeId(100);
            let data = vec![1, 2, 3, 4, 5];
            let timestamp = Timestamp(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64);
            let node = Node {
                id,
                type_id,
                timestamp,
                data: data.clone(),
            };
            storage.store_node(&node).unwrap();

            // Verify the node exists in memory
            println!("Verifying node exists in memory...");
            let retrieved_node = storage.get_node(id).unwrap().unwrap();
            assert_eq!(retrieved_node.data, data);

            // Flush all pages to disk
            println!("Flushing all pages to disk...");
            storage.flush_all().unwrap();
            println!("Flush complete.");

            // Create a new storage manager with the same database file
            println!("Creating new storage manager...");
            let storage2 = StorageManager::new(
                db_path.clone(),
                DEFAULT_PAGE_SIZE,
                NonZeroUsize::new(10).unwrap(),
                None, // Unlimited for tests
                1000, // 1 second flush interval
            ).unwrap();

            // The node should be loaded from disk pages on startup
            println!("Verifying node was loaded from disk...");
            let loaded_node = storage2.get_node(id).unwrap();
            assert!(loaded_node.is_some(), "Node should be loaded from disk pages");
            assert_eq!(loaded_node.unwrap().data, data);

            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_simple_node_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_simple_node_persistence...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("simple_node_test.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited for tests
                flush_interval_ms: 1000, // 1 second flush interval
            };

            // First server instance
            println!("Creating first server instance...");
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Add a node
            println!("Adding a node...");
            let id = 42;
            let type_id = 100;
            let data = vec![1, 2, 3, 4, 5];
            api1.obj_add(id, type_id, data.clone()).await.unwrap();

            // Verify the node exists
            println!("Verifying node exists...");
            let node = api1.obj_get(id).await.unwrap().unwrap();
            assert_eq!(node.data, data);

            // Shutdown the server normally
            println!("Shutting down server...");
            server1.shutdown().await.unwrap();
            println!("Server shutdown complete.");

            // Create a new server instance with the same database file
            println!("Creating second server instance...");
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // The node should be loaded from disk pages on server start
            println!("Verifying node was loaded from disk...");
            let loaded_node = api2.obj_get(id).await.unwrap();
            assert!(loaded_node.is_some(), "Node should be loaded from disk pages");
            assert_eq!(loaded_node.unwrap().data, data);

            // Clean up
            println!("Cleaning up...");
            server2.shutdown().await.unwrap();
            println!("Test completed successfully.");
        });
    }

    #[test]
    fn test_page_based_storage_without_transaction_log() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting test_page_based_storage_without_transaction_log...");
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("page_storage_test.db");
            let log_path = temp_dir.path().join("page_storage_test.log");
            let seq_path = temp_dir.path().join("page_storage_test.seq");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited for tests
                flush_interval_ms: 1000, // 1 second flush interval
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Add a node
            let id = 42;
            let type_id = 100;
            let data = vec![1, 2, 3, 4, 5];
            api1.obj_add(id, type_id, data.clone()).await.unwrap();

            // Add an arc
            let source_id = 42;
            let arc_type = 200;
            let target_id = 43;
            let arc_data = vec![10, 20, 30];

            // First create the target node
            api1.obj_add(target_id, type_id, vec![5, 4, 3, 2, 1]).await.unwrap();

            // Then create the arc
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            api1.assoc_add(source_id, arc_type, target_id, timestamp, arc_data.clone()).await.unwrap();

            // Verify the node and arc exist in memory
            println!("Verifying node and arc exist in memory...");
            let node = api1.obj_get(id).await.unwrap().unwrap();
            assert_eq!(node.data, data);

            let arc = api1.assoc_get(source_id, arc_type, target_id).await.unwrap().unwrap();
            assert_eq!(arc.data, arc_data);

            // Shutdown the server normally to ensure all data is flushed to disk
            println!("Shutting down server normally to flush all data to disk...");
            server1.shutdown().await.unwrap();
            println!("Server shutdown complete.");

            // Delete the transaction log and sequence file to force loading from pages
            println!("Deleting transaction log to force loading from pages...");
            if fs::metadata(&log_path).is_ok() {
                println!("Found transaction log file, deleting it...");
                fs::remove_file(&log_path).unwrap();
                println!("Transaction log file deleted.");
            } else {
                println!("Transaction log file not found at {}", log_path.display());
            }

            if fs::metadata(&seq_path).is_ok() {
                println!("Found sequence file, deleting it...");
                fs::remove_file(&seq_path).unwrap();
                println!("Sequence file deleted.");
            } else {
                println!("Sequence file not found at {}", seq_path.display());
            }

            // Create a new server instance with the same database file
            println!("Starting new server without transaction log...");
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // The node and arc should be loaded from disk pages on server start
            // No need to call recover() since we want to test page-based loading

            // Verify the node still exists (should be loaded from disk pages)
            println!("Verifying node was loaded from disk pages...");
            let loaded_node = api2.obj_get(id).await.unwrap();
            assert!(loaded_node.is_some(), "Node should be loaded from disk pages");
            assert_eq!(loaded_node.unwrap().data, data);

            // Verify the arc still exists (should be loaded from disk pages)
            println!("Verifying arc was loaded from disk pages...");
            let loaded_arc = api2.assoc_get(source_id, arc_type, target_id).await.unwrap();
            assert!(loaded_arc.is_some(), "Arc should be loaded from disk pages");
            assert_eq!(loaded_arc.unwrap().data, arc_data);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }
}
