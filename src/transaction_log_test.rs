//! Tests for the transaction log operations

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use temp_dir::TempDir;
    use tokio::runtime::Runtime;

    use crate::server::ServerConfig;
    use crate::storage::DEFAULT_PAGE_SIZE;
    use crate::{Server, YarnCacheApi};

    #[test]
    fn test_node_operations_in_transaction_log() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test_node_txlog.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited for tests
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Test obj_add
            let id = 1;
            let type_id = 2;
            let data = vec![1, 2, 3, 4];
            let _node = api1.obj_add(id, type_id, data.clone()).await.unwrap();

            // Test obj_update
            let updated_data = vec![5, 6, 7, 8];
            let _updated_node = api1.obj_update(id, updated_data.clone()).await.unwrap();

            // Test obj_delete
            let deleted = api1.obj_delete(id).await.unwrap();
            assert!(deleted);

            // Simulate a crash
            server1.shutdown().await.unwrap();

            // Create a new server instance
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the node was deleted (last operation in the log)
            let node = api2.obj_get(id).await.unwrap();
            assert!(node.is_none());

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_arc_operations_in_transaction_log() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test_arc_txlog.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited for tests
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
            api1.obj_add(source_id, node_type, vec![1, 2, 3])
                .await
                .unwrap();
            api1.obj_add(target_id, node_type, vec![4, 5, 6])
                .await
                .unwrap();

            // Test assoc_add
            let timestamp = 12345;
            let arc_data = vec![7, 8, 9];
            let _arc = api1
                .assoc_add(source_id, arc_type, target_id, timestamp, arc_data.clone())
                .await
                .unwrap();

            // Test assoc_update
            let updated_arc_data = vec![10, 11, 12];
            let _updated_arc = api1
                .assoc_update(source_id, arc_type, target_id, updated_arc_data.clone())
                .await
                .unwrap();

            // Simulate a crash
            server1.shutdown().await.unwrap();

            // Create a new server instance
            let server2 = Server::with_config(config.clone()).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the arc was updated
            let recovered_arc = api2
                .assoc_get(source_id, arc_type, target_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(recovered_arc.data, updated_arc_data);

            // Test assoc_delete
            let deleted = api2
                .assoc_delete(source_id, arc_type, target_id)
                .await
                .unwrap();
            assert!(deleted);

            // Simulate another crash
            server2.shutdown().await.unwrap();

            // Create a third server instance
            let server3 = Server::with_config(config).await.unwrap();
            let api3 = YarnCacheApi::new(Arc::new(server3.clone()));

            // Recover the database
            api3.recover().await.unwrap();

            // Verify the arc was deleted
            let deleted_arc = api3
                .assoc_get(source_id, arc_type, target_id)
                .await
                .unwrap();
            assert!(deleted_arc.is_none());

            // Clean up
            server3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_mixed_operations_in_transaction_log() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test_mixed_txlog.db");

            // Create a server
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited for tests
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            // Create multiple nodes and associations
            let node_ids = vec![1, 2, 3];
            let node_type = 10;

            // Add nodes
            for &id in &node_ids {
                api1.obj_add(id, node_type, vec![id as u8]).await.unwrap();
            }

            // Create associations
            let arc_type = 20;
            api1.assoc_add(1, arc_type, 2, 1000, vec![1, 2])
                .await
                .unwrap();
            api1.assoc_add(2, arc_type, 3, 2000, vec![2, 3])
                .await
                .unwrap();

            // Update a node
            api1.obj_update(1, vec![9, 9, 9]).await.unwrap();

            // Update an association
            api1.assoc_update(1, arc_type, 2, vec![8, 8, 8])
                .await
                .unwrap();

            // Delete a node
            api1.obj_delete(3).await.unwrap();

            // Delete an association
            api1.assoc_delete(2, arc_type, 3).await.unwrap();

            // Simulate a crash
            server1.shutdown().await.unwrap();

            // Create a new server instance
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Recover the database
            api2.recover().await.unwrap();

            // Verify node operations
            let node1 = api2.obj_get(1).await.unwrap().unwrap();
            assert_eq!(node1.data, vec![9, 9, 9]); // Updated

            let node2 = api2.obj_get(2).await.unwrap().unwrap();
            assert_eq!(node2.data, vec![2]); // Original

            let node3 = api2.obj_get(3).await.unwrap();
            assert!(node3.is_none()); // Deleted

            // Verify association operations
            let arc12 = api2.assoc_get(1, arc_type, 2).await.unwrap().unwrap();
            assert_eq!(arc12.data, vec![8, 8, 8]); // Updated

            let arc23 = api2.assoc_get(2, arc_type, 3).await.unwrap();
            assert!(arc23.is_none()); // Deleted

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }
}
