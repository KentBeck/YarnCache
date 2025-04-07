//! Tests for disk space limits

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use temp_dir::TempDir;
    use tokio::runtime::Runtime;

    use crate::server::ServerConfig;
    use crate::storage::DEFAULT_PAGE_SIZE;
    use crate::{Error, Server, YarnCacheApi};

    #[test]
    fn test_disk_space_limit() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("disk_space_test.db");

            // Create a server with a very small disk space limit (1KB)
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: Some(1024), // 1KB limit
            };

            let server = Server::with_config(config).await.unwrap();
            let api = YarnCacheApi::new(Arc::new(server.clone()));

            // Add a small object (should succeed)
            let small_data = vec![0; 100]; // 100 bytes
            api.obj_add(1, 1, small_data.clone()).await.unwrap();

            // Add another small object (should succeed)
            let small_data2 = vec![1; 200]; // 200 bytes
            api.obj_add(2, 1, small_data2.clone()).await.unwrap();

            // Try to add a large object (should fail)
            let large_data = vec![2; 900]; // 900 bytes, which would exceed the 1KB limit
            let result = api.obj_add(3, 1, large_data.clone()).await;

            // Verify that we got a DiskSpaceExceeded error
            match result {
                Err(Error::DiskSpaceExceeded(_)) => {
                    // This is the expected error
                    println!("Got expected DiskSpaceExceeded error");
                }
                Ok(_) => {
                    panic!("Expected DiskSpaceExceeded error, but operation succeeded");
                }
                Err(e) => {
                    panic!("Expected DiskSpaceExceeded error, but got: {:?}", e);
                }
            }

            // Verify that the first two objects are still there
            let obj1 = api.obj_get(1).await.unwrap().unwrap();
            assert_eq!(obj1.data, small_data);

            let obj2 = api.obj_get(2).await.unwrap().unwrap();
            assert_eq!(obj2.data, small_data2);

            // Verify that the third object was not added
            let obj3 = api.obj_get(3).await.unwrap();
            assert!(obj3.is_none());

            // Clean up
            server.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_disk_space_update_limit() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("update_limit_test.db");

            // Create a server with a small disk space limit (1KB)
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: Some(1024), // 1KB limit
            };

            let server = Server::with_config(config).await.unwrap();
            let api = YarnCacheApi::new(Arc::new(server.clone()));

            // Add a small object
            let small_data = vec![0; 300]; // 300 bytes
            api.obj_add(1, 1, small_data.clone()).await.unwrap();

            // Update with slightly larger data (should succeed)
            let medium_data = vec![1; 400]; // 400 bytes
            api.obj_update(1, medium_data.clone()).await.unwrap();

            // Try to update with very large data (should fail)
            let large_data = vec![2; 800]; // 800 bytes, which would exceed the 1KB limit
            let result = api.obj_update(1, large_data.clone()).await;

            // Verify that we got a DiskSpaceExceeded error
            match result {
                Err(Error::DiskSpaceExceeded(_)) => {
                    // This is the expected error
                    println!("Got expected DiskSpaceExceeded error");
                }
                Ok(_) => {
                    panic!("Expected DiskSpaceExceeded error, but operation succeeded");
                }
                Err(e) => {
                    panic!("Expected DiskSpaceExceeded error, but got: {:?}", e);
                }
            }

            // Verify that the object still has the medium data
            let obj = api.obj_get(1).await.unwrap().unwrap();
            assert_eq!(obj.data, medium_data);

            // Clean up
            server.shutdown().await.unwrap();
        });
    }

    #[test]
    fn test_unlimited_disk_space() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("unlimited_test.db");

            // Create a server with unlimited disk space (default)
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10).unwrap(),
                max_disk_space: None, // Unlimited
            };

            let server = Server::with_config(config).await.unwrap();
            let api = YarnCacheApi::new(Arc::new(server.clone()));

            // Add a large object (should succeed)
            let large_data = vec![0; 10000]; // 10KB
            api.obj_add(1, 1, large_data.clone()).await.unwrap();

            // Verify that the object was added
            let obj = api.obj_get(1).await.unwrap().unwrap();
            assert_eq!(obj.data, large_data);

            // Clean up
            server.shutdown().await.unwrap();
        });
    }
}
