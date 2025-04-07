//! Tests for object persistence across server restarts

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
    fn test_object_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("persistence_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create some objects
            let obj1_id = 101;
            let obj2_id = 202;
            let obj3_id = 303;
            let type_id = 1;

            let obj1_data = vec![1, 2, 3];
            let obj2_data = vec![4, 5, 6];
            let obj3_data = vec![7, 8, 9];

            // Add the objects
            api1.obj_add(obj1_id, type_id, obj1_data.clone()).await.unwrap();
            api1.obj_add(obj2_id, type_id, obj2_data.clone()).await.unwrap();
            api1.obj_add(obj3_id, type_id, obj3_data.clone()).await.unwrap();

            // Verify the objects were created
            let retrieved_obj1 = api1.obj_get(obj1_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj1.data, obj1_data);

            let retrieved_obj2 = api1.obj_get(obj2_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj2.data, obj2_data);

            let retrieved_obj3 = api1.obj_get(obj3_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj3.data, obj3_data);

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the objects persisted
            let retrieved_obj1 = api2.obj_get(obj1_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj1.data, obj1_data);

            let retrieved_obj2 = api2.obj_get(obj2_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj2.data, obj2_data);

            let retrieved_obj3 = api2.obj_get(obj3_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj3.data, obj3_data);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_object_update_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("update_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create an object
            let obj_id = 101;
            let type_id = 1;
            let initial_data = vec![1, 2, 3];

            // Add the object
            api1.obj_add(obj_id, type_id, initial_data.clone()).await.unwrap();

            // Verify the object was created
            let retrieved_obj = api1.obj_get(obj_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj.data, initial_data);

            // Update the object
            let updated_data = vec![4, 5, 6];
            api1.obj_update(obj_id, updated_data.clone()).await.unwrap();

            // Verify the update
            let retrieved_obj = api1.obj_get(obj_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj.data, updated_data);

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the updated object persisted
            let retrieved_obj = api2.obj_get(obj_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj.data, updated_data);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_object_delete_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("delete_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create objects
            let obj1_id = 101;
            let obj2_id = 202;
            let type_id = 1;

            let obj1_data = vec![1, 2, 3];
            let obj2_data = vec![4, 5, 6];

            // Add the objects
            api1.obj_add(obj1_id, type_id, obj1_data.clone()).await.unwrap();
            api1.obj_add(obj2_id, type_id, obj2_data.clone()).await.unwrap();

            // Verify the objects were created
            let retrieved_obj1 = api1.obj_get(obj1_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj1.data, obj1_data);

            let retrieved_obj2 = api1.obj_get(obj2_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj2.data, obj2_data);

            // Delete one object
            let deleted = api1.obj_delete(obj1_id).await.unwrap();
            assert!(deleted);

            // Verify the deletion
            let retrieved_obj1 = api1.obj_get(obj1_id).await.unwrap();
            assert!(retrieved_obj1.is_none());

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify the deletion persisted
            let retrieved_obj1 = api2.obj_get(obj1_id).await.unwrap();
            assert!(retrieved_obj1.is_none());

            // Verify the other object still exists
            let retrieved_obj2 = api2.obj_get(obj2_id).await.unwrap().unwrap();
            assert_eq!(retrieved_obj2.data, obj2_data);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_mixed_operations_persistence() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("mixed_test.db");

            // Create the first server instance
            let (server1, api1) = create_server(db_path.clone()).await;

            // Create objects
            let type_id = 1;
            let objects = vec![
                (101, vec![1, 1, 1]),
                (202, vec![2, 2, 2]),
                (303, vec![3, 3, 3]),
                (404, vec![4, 4, 4]),
                (505, vec![5, 5, 5]),
            ];

            // Add all objects
            for (id, data) in &objects {
                api1.obj_add(*id, type_id, data.clone()).await.unwrap();
            }

            // Update some objects
            api1.obj_update(202, vec![22, 22, 22]).await.unwrap();
            api1.obj_update(404, vec![44, 44, 44]).await.unwrap();

            // Delete some objects
            api1.obj_delete(101).await.unwrap();
            api1.obj_delete(505).await.unwrap();

            // Shutdown the first server
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            let (server2, api2) = create_server(db_path.clone()).await;

            // Recover the database
            api2.recover().await.unwrap();

            // Verify deleted objects are gone
            assert!(api2.obj_get(101).await.unwrap().is_none());
            assert!(api2.obj_get(505).await.unwrap().is_none());

            // Verify updated objects have new data
            let obj202 = api2.obj_get(202).await.unwrap().unwrap();
            assert_eq!(obj202.data, vec![22, 22, 22]);

            let obj404 = api2.obj_get(404).await.unwrap().unwrap();
            assert_eq!(obj404.data, vec![44, 44, 44]);

            // Verify untouched object is unchanged
            let obj303 = api2.obj_get(303).await.unwrap().unwrap();
            assert_eq!(obj303.data, vec![3, 3, 3]);

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }
}
