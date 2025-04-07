//! Tests for the transaction log recovery

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use temp_dir::TempDir;
    use tokio::runtime::Runtime;

    use crate::{Server, YarnCacheApi};
    use crate::server::ServerConfig;
    use crate::storage::DEFAULT_PAGE_SIZE;

    #[test]
    fn test_recovery_after_crash() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Add a node
            let id = 1;
            let type_id = 2;
            let data = vec![1, 2, 3, 4];
            api1.obj_add(id, type_id, data.clone()).await.unwrap();

            // Verify the node exists
            let node = api1.obj_get(id).await.unwrap().unwrap();
            assert_eq!(node.data, data);

            // Simulate a crash by shutting down the server without flushing
            // In a real scenario, this would be an abrupt termination
            server1.shutdown().await.unwrap();

            // Create a new server instance with the same database file
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Before recovery, the node might not exist (depends on implementation)
            // But after recovery, it should definitely exist

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the node exists after recovery
            let recovered_node = api2.obj_get(id).await.unwrap().unwrap();
            assert_eq!(recovered_node.data, data);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_recovery_with_multiple_operations() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test2.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Add multiple nodes
            for i in 1..=5 {
                let id = i;
                let type_id = i * 10;
                let data = vec![i as u8; i as usize];
                api1.obj_add(id, type_id, data).await.unwrap();
            }

            // Update a node
            let updated_data = vec![10, 20, 30];
            api1.obj_update(3, updated_data.clone()).await.unwrap();

            // Delete a node
            api1.obj_delete(5).await.unwrap();

            // Simulate a crash
            server1.shutdown().await.unwrap();

            // Create a new server instance
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Recover the database
            api2.recover().await.unwrap();

            // Verify all operations were recovered correctly

            // Check nodes 1, 2, and 4 exist with original data
            for i in [1, 2, 4] {
                let node = api2.obj_get(i).await.unwrap().unwrap();
                assert_eq!(node.data, vec![i as u8; i as usize]);
            }

            // Check node 3 was updated
            let node3 = api2.obj_get(3).await.unwrap().unwrap();
            assert_eq!(node3.data, updated_data);

            // Check node 5 was deleted
            let node5 = api2.obj_get(5).await.unwrap();
            assert!(node5.is_none());

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_recovery_of_associations() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test_assoc.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Create source and target nodes
            let source_id = 101;
            let target_id = 202;
            let node_type = 1;
            let arc_type = 2;

            // Add the nodes
            api1.obj_add(source_id, node_type, vec![1, 2, 3]).await.unwrap();
            api1.obj_add(target_id, node_type, vec![4, 5, 6]).await.unwrap();

            // Create an association
            let timestamp = 12345;
            let arc_data = vec![7, 8, 9];
            let _arc = api1.assoc_add(source_id, arc_type, target_id, timestamp, arc_data.clone()).await.unwrap();

            // Verify the association was created
            let retrieved_arc = api1.assoc_get(source_id, arc_type, target_id).await.unwrap().unwrap();
            assert_eq!(retrieved_arc.from_node.0, source_id);
            assert_eq!(retrieved_arc.to_node.0, target_id);
            assert_eq!(retrieved_arc.type_id.0, arc_type);
            assert_eq!(retrieved_arc.data, arc_data);

            // Simulate a crash
            server1.shutdown().await.unwrap();

            // Create a new server instance
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the nodes were recovered
            let source_node = api2.obj_get(source_id).await.unwrap().unwrap();
            assert_eq!(source_node.id.0, source_id);

            let target_node = api2.obj_get(target_id).await.unwrap().unwrap();
            assert_eq!(target_node.id.0, target_id);

            // Verify the association was recovered
            let recovered_arc = api2.assoc_get(source_id, arc_type, target_id).await.unwrap().unwrap();
            assert_eq!(recovered_arc.from_node.0, source_id);
            assert_eq!(recovered_arc.to_node.0, target_id);
            assert_eq!(recovered_arc.type_id.0, arc_type);
            assert_eq!(recovered_arc.timestamp.0, timestamp);
            assert_eq!(recovered_arc.data, arc_data);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }
}
