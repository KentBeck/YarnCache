//! Tests for the YarnCache API

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
    fn test_object_operations() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test.db");

            // Create a server
            let config = ServerConfig {
                db_path,
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited for tests
            };
            let server = Server::with_config(config).await.unwrap();
            let server_arc = Arc::new(server);

            // Create an API instance
            let api = YarnCacheApi::new(server_arc.clone());

            // Test obj_add
            let id = 1;
            let type_id = 2;
            let data = vec![1, 2, 3, 4];
            let node = api.obj_add(id, type_id, data.clone()).await.unwrap();

            assert_eq!(node.id.0, id);
            assert_eq!(node.type_id.0, type_id);
            assert_eq!(node.data, data);

            // Test obj_get
            let retrieved_node = api.obj_get(id).await.unwrap().unwrap();
            assert_eq!(retrieved_node.id.0, id);
            assert_eq!(retrieved_node.type_id.0, type_id);
            assert_eq!(retrieved_node.data, data);

            // Test obj_update
            let new_data = vec![5, 6, 7, 8];
            let updated_node = api.obj_update(id, new_data.clone()).await.unwrap();
            assert_eq!(updated_node.id.0, id);
            assert_eq!(updated_node.type_id.0, type_id);
            assert_eq!(updated_node.data, new_data);

            // Verify the update
            let retrieved_node = api.obj_get(id).await.unwrap().unwrap();
            assert_eq!(retrieved_node.data, new_data);

            // Test obj_delete
            let deleted = api.obj_delete(id).await.unwrap();
            assert!(deleted);

            // Verify the deletion
            let retrieved_node = api.obj_get(id).await.unwrap();
            assert!(retrieved_node.is_none());

            // Test deleting a non-existent node
            let deleted = api.obj_delete(999).await.unwrap();
            assert!(!deleted);

            // Shutdown the server
            server_arc.shutdown().await.unwrap();
        });
    }
}
