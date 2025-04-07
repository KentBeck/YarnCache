//! Tests for arc persistence across server restarts

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::path::PathBuf;
    use temp_dir::TempDir;
    use tokio::runtime::Runtime;

    use crate::{Server, YarnCacheApi};
    use crate::server::ServerConfig;
    use crate::storage::DEFAULT_PAGE_SIZE;
    use crate::types::Timestamp;

    /// Helper function to create a server with a given database path
    async fn create_server(db_path: PathBuf) -> (Arc<Server>, YarnCacheApi) {
        let config = ServerConfig {
            db_path,
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: NonZeroUsize::new(10).unwrap(),
            max_disk_space: None, // Unlimited for tests
        };

        let server = Server::with_config(config).await.unwrap();
        let api = YarnCacheApi::new(Arc::new(server.clone()));

        (Arc::new(server), api)
    }

    #[test]
    fn test_basic_arc_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("basic_arc_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create nodes
            let source_id = 101;
            let target_id = 202;
            let node_type = 1;
            let arc_type = 2;
            let timestamp = 12345;

            // Add the nodes
            api1.obj_add(source_id, node_type, vec![1, 2, 3]).await.unwrap();
            api1.obj_add(target_id, node_type, vec![4, 5, 6]).await.unwrap();

            // Create an association
            let arc_data = vec![7, 8, 9];
            let arc = api1.assoc_add(source_id, arc_type, target_id, timestamp, arc_data.clone()).await.unwrap();

            // Verify the association was created
            assert_eq!(arc.from_node.0, source_id);
            assert_eq!(arc.to_node.0, target_id);
            assert_eq!(arc.type_id.0, arc_type);
            assert_eq!(arc.timestamp.0, timestamp);
            assert_eq!(arc.data, arc_data);

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

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

    #[test]
    fn test_arc_update_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("arc_update_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create nodes
            let source_id = 101;
            let target_id = 202;
            let node_type = 1;
            let arc_type = 2;
            let timestamp = 12345;

            // Add the nodes
            api1.obj_add(source_id, node_type, vec![1, 2, 3]).await.unwrap();
            api1.obj_add(target_id, node_type, vec![4, 5, 6]).await.unwrap();

            // Create an association
            let initial_arc_data = vec![7, 8, 9];
            let arc = api1.assoc_add(source_id, arc_type, target_id, timestamp, initial_arc_data.clone()).await.unwrap();

            // Verify the association was created
            assert_eq!(arc.data, initial_arc_data);

            // Update the association
            let updated_arc_data = vec![10, 11, 12];
            let updated_arc = api1.assoc_update(source_id, arc_type, target_id, updated_arc_data.clone()).await.unwrap();

            // Verify the update
            assert_eq!(updated_arc.data, updated_arc_data);

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the association was recovered with updated data
            let recovered_arc = api2.assoc_get(source_id, arc_type, target_id).await.unwrap().unwrap();
            assert_eq!(recovered_arc.from_node.0, source_id);
            assert_eq!(recovered_arc.to_node.0, target_id);
            assert_eq!(recovered_arc.type_id.0, arc_type);
            assert_eq!(recovered_arc.timestamp.0, timestamp);
            assert_eq!(recovered_arc.data, updated_arc_data); // Should have the updated data

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_arc_delete_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("arc_delete_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create nodes
            let source_id = 101;
            let target1_id = 202;
            let target2_id = 303;
            let node_type = 1;
            let arc_type = 2;
            let timestamp = 12345;

            // Add the nodes
            api1.obj_add(source_id, node_type, vec![1, 2, 3]).await.unwrap();
            api1.obj_add(target1_id, node_type, vec![4, 5, 6]).await.unwrap();
            api1.obj_add(target2_id, node_type, vec![7, 8, 9]).await.unwrap();

            // Create two associations
            api1.assoc_add(source_id, arc_type, target1_id, timestamp, vec![10, 11, 12]).await.unwrap();
            api1.assoc_add(source_id, arc_type, target2_id, timestamp, vec![13, 14, 15]).await.unwrap();

            // Verify both associations exist
            let arc1 = api1.assoc_get(source_id, arc_type, target1_id).await.unwrap();
            let arc2 = api1.assoc_get(source_id, arc_type, target2_id).await.unwrap();
            assert!(arc1.is_some());
            assert!(arc2.is_some());

            // Delete one association
            let deleted = api1.assoc_delete(source_id, arc_type, target1_id).await.unwrap();
            assert!(deleted);

            // Verify it was deleted
            let arc1_after_delete = api1.assoc_get(source_id, arc_type, target1_id).await.unwrap();
            assert!(arc1_after_delete.is_none());

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the deletion persisted
            let recovered_arc1 = api2.assoc_get(source_id, arc_type, target1_id).await.unwrap();
            assert!(recovered_arc1.is_none());

            // Verify the other association still exists
            let recovered_arc2 = api2.assoc_get(source_id, arc_type, target2_id).await.unwrap();
            assert!(recovered_arc2.is_some());

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_multiple_arc_types_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("multiple_arc_types_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create nodes
            let source_id = 101;
            let target_id = 202;
            let node_type = 1;
            let arc_type1 = 2;
            let arc_type2 = 3;
            let timestamp = 12345;

            // Add the nodes
            api1.obj_add(source_id, node_type, vec![1, 2, 3]).await.unwrap();
            api1.obj_add(target_id, node_type, vec![4, 5, 6]).await.unwrap();

            // Create associations with different types
            let arc_data1 = vec![7, 8, 9];
            let arc_data2 = vec![10, 11, 12];
            api1.assoc_add(source_id, arc_type1, target_id, timestamp, arc_data1.clone()).await.unwrap();
            api1.assoc_add(source_id, arc_type2, target_id, timestamp, arc_data2.clone()).await.unwrap();

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify both associations were recovered
            let recovered_arc1 = api2.assoc_get(source_id, arc_type1, target_id).await.unwrap().unwrap();
            let recovered_arc2 = api2.assoc_get(source_id, arc_type2, target_id).await.unwrap().unwrap();

            assert_eq!(recovered_arc1.type_id.0, arc_type1);
            assert_eq!(recovered_arc1.data, arc_data1);

            assert_eq!(recovered_arc2.type_id.0, arc_type2);
            assert_eq!(recovered_arc2.data, arc_data2);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_complex_graph_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("complex_graph_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create a small graph with multiple nodes and relationships
            // Node IDs
            let node_ids = vec![101, 102, 103, 104, 105];
            let node_type = 1;

            // Arc types
            let follows_type = 10;
            let likes_type = 20;
            let created_type = 30;

            // Create all nodes
            for (i, &id) in node_ids.iter().enumerate() {
                api1.obj_add(id, node_type, vec![i as u8, (i+1) as u8, (i+2) as u8]).await.unwrap();
            }

            // Create a complex graph structure:
            // 101 follows 102, 103
            // 102 follows 104
            // 103 follows 102, 105
            // 104 follows 105
            // 101 likes 104, 105
            // 102 likes 103
            // 103 created 105

            let timestamp = Timestamp(12345);

            // Follows relationships
            api1.assoc_add(101, follows_type, 102, timestamp.0, vec![1]).await.unwrap();
            api1.assoc_add(101, follows_type, 103, timestamp.0, vec![2]).await.unwrap();
            api1.assoc_add(102, follows_type, 104, timestamp.0, vec![3]).await.unwrap();
            api1.assoc_add(103, follows_type, 102, timestamp.0, vec![4]).await.unwrap();
            api1.assoc_add(103, follows_type, 105, timestamp.0, vec![5]).await.unwrap();
            api1.assoc_add(104, follows_type, 105, timestamp.0, vec![6]).await.unwrap();

            // Likes relationships
            api1.assoc_add(101, likes_type, 104, timestamp.0, vec![7]).await.unwrap();
            api1.assoc_add(101, likes_type, 105, timestamp.0, vec![8]).await.unwrap();
            api1.assoc_add(102, likes_type, 103, timestamp.0, vec![9]).await.unwrap();

            // Created relationships
            api1.assoc_add(103, created_type, 105, timestamp.0, vec![10]).await.unwrap();

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify all nodes were recovered
            for &id in &node_ids {
                let node = api2.obj_get(id).await.unwrap();
                assert!(node.is_some());
            }

            // Verify all relationships were recovered

            // Follows relationships
            assert!(api2.assoc_get(101, follows_type, 102).await.unwrap().is_some());
            assert!(api2.assoc_get(101, follows_type, 103).await.unwrap().is_some());
            assert!(api2.assoc_get(102, follows_type, 104).await.unwrap().is_some());
            assert!(api2.assoc_get(103, follows_type, 102).await.unwrap().is_some());
            assert!(api2.assoc_get(103, follows_type, 105).await.unwrap().is_some());
            assert!(api2.assoc_get(104, follows_type, 105).await.unwrap().is_some());

            // Likes relationships
            assert!(api2.assoc_get(101, likes_type, 104).await.unwrap().is_some());
            assert!(api2.assoc_get(101, likes_type, 105).await.unwrap().is_some());
            assert!(api2.assoc_get(102, likes_type, 103).await.unwrap().is_some());

            // Created relationships
            assert!(api2.assoc_get(103, created_type, 105).await.unwrap().is_some());

            // Verify some specific data
            let arc = api2.assoc_get(103, created_type, 105).await.unwrap().unwrap();
            assert_eq!(arc.data, vec![10]);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_bidirectional_arcs_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("bidirectional_arcs_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create nodes
            let node1_id = 101;
            let node2_id = 202;
            let node_type = 1;

            // Use different arc types for each direction
            let outgoing_arc_type = 2;
            let incoming_arc_type = 3;
            let timestamp = 12345;

            // Add the nodes
            api1.obj_add(node1_id, node_type, vec![1, 2, 3]).await.unwrap();
            api1.obj_add(node2_id, node_type, vec![4, 5, 6]).await.unwrap();

            // Create bidirectional associations with different types
            let outgoing_data = vec![7, 8, 9];
            let incoming_data = vec![10, 11, 12];

            // Node1 -> Node2 (outgoing)
            api1.assoc_add(node1_id, outgoing_arc_type, node2_id, timestamp, outgoing_data.clone()).await.unwrap();

            // Node2 -> Node1 (incoming)
            api1.assoc_add(node2_id, incoming_arc_type, node1_id, timestamp, incoming_data.clone()).await.unwrap();

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify both arcs were recovered
            let outgoing_arc = api2.assoc_get(node1_id, outgoing_arc_type, node2_id).await.unwrap().unwrap();
            let incoming_arc = api2.assoc_get(node2_id, incoming_arc_type, node1_id).await.unwrap().unwrap();

            // Check the data matches what we stored
            assert_eq!(outgoing_arc.data, outgoing_data);
            assert_eq!(incoming_arc.data, incoming_data);

            // Check the from/to nodes are correct
            assert_eq!(outgoing_arc.from_node.0, node1_id);
            assert_eq!(outgoing_arc.to_node.0, node2_id);
            assert_eq!(outgoing_arc.type_id.0, outgoing_arc_type);

            assert_eq!(incoming_arc.from_node.0, node2_id);
            assert_eq!(incoming_arc.to_node.0, node1_id);
            assert_eq!(incoming_arc.type_id.0, incoming_arc_type);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }
}
