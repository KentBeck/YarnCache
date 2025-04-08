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
    use crate::storage::{DEFAULT_PAGE_SIZE, StorageManager};
    use crate::types::{Node, NodeId, TypeId, Timestamp};
    use crate::{Server, YarnCacheApi};

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
