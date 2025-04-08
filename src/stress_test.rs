//! Stress test for YarnCache

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use temp_dir::TempDir;
    use tokio::runtime::Runtime;
    use rand::prelude::*;
    use rand::distributions::Uniform;

    use crate::{Server, YarnCacheApi};
    use crate::server::ServerConfig;
    use crate::storage::DEFAULT_PAGE_SIZE;

    const NUM_NODES: usize = 1000;
    const NUM_ARCS: usize = 5000;
    const TEST_DURATION_SECS: u64 = 60;
    const NODE_TYPE: u64 = 1;
    const ARC_TYPES: [u64; 5] = [10, 20, 30, 40, 50]; // Different arc types for variety

    /// Helper function to create a server with a given database path
    async fn create_test_server() -> (Arc<Server>, YarnCacheApi) {
        // Create a temporary directory for the database
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("stress_test.db");

        // Create a server with a larger cache size for better performance
        let config = ServerConfig {
            db_path,
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: NonZeroUsize::new(10000).unwrap(), // Larger cache for stress test
            max_disk_space: None, // Unlimited for stress test
            flush_interval_ms: 1000, // 1 second flush interval
        };

        let server = Server::with_config(config).await.unwrap();
        let api = YarnCacheApi::new(Arc::new(server.clone()));

        (Arc::new(server), api)
    }

    /// Generate random data of a given size
    fn generate_random_data(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; size];
        rng.fill(&mut data[..]);
        data
    }

    #[test]
    #[ignore] // Ignore by default as it's a long-running test
    fn stress_test_performance() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting stress test...");
            println!("Creating server...");

            // Create the server
            let (server, api) = create_test_server().await;

            // Create nodes
            println!("Creating {} nodes...", NUM_NODES);
            let node_ids: Vec<u64> = (1..=NUM_NODES as u64).collect();

            let start_time = Instant::now();

            for &node_id in &node_ids {
                // Generate random data between 64 and 256 bytes
                let data_size = thread_rng().gen_range(64..=256);
                let data = generate_random_data(data_size);

                api.obj_add(node_id, NODE_TYPE, data).await.unwrap();
            }

            let node_creation_time = start_time.elapsed();
            println!("Created {} nodes in {:.2?} ({:.2} nodes/sec)",
                NUM_NODES,
                node_creation_time,
                NUM_NODES as f64 / node_creation_time.as_secs_f64());

            // Create arcs
            println!("Creating {} arcs...", NUM_ARCS);
            let arc_start_time = Instant::now();
            let mut rng = thread_rng();

            // Distribution for selecting random nodes
            let node_dist = Uniform::from(0..node_ids.len());
            // Distribution for selecting random arc types
            let arc_type_dist = Uniform::from(0..ARC_TYPES.len());

            for _ in 0..NUM_ARCS {
                // Select random source and target nodes
                let source_idx = rng.sample(node_dist);
                let target_idx = rng.sample(node_dist);
                let source_id = node_ids[source_idx];
                let target_id = node_ids[target_idx];

                // Select random arc type
                let arc_type_idx = rng.sample(arc_type_dist);
                let arc_type = ARC_TYPES[arc_type_idx];

                // Generate random timestamp
                let timestamp = rng.gen_range(1..=100000);

                // Generate random data between 32 and 128 bytes
                let data_size = rng.gen_range(32..=128);
                let data = generate_random_data(data_size);

                // Create the arc
                api.assoc_add(source_id, arc_type, target_id, timestamp, data).await.unwrap();
            }

            let arc_creation_time = arc_start_time.elapsed();
            println!("Created {} arcs in {:.2?} ({:.2} arcs/sec)",
                NUM_ARCS,
                arc_creation_time,
                NUM_ARCS as f64 / arc_creation_time.as_secs_f64());

            // Run queries for a minute
            println!("Running queries for {} seconds...", TEST_DURATION_SECS);
            let query_start_time = Instant::now();
            let end_time = query_start_time + Duration::from_secs(TEST_DURATION_SECS);

            let mut total_queries = 0;
            let mut node_queries = 0;
            let mut arc_queries = 0;

            while Instant::now() < end_time {
                // Randomly choose between node and arc queries
                let query_type = rng.gen_bool(0.5);

                if query_type {
                    // Node query
                    let node_idx = rng.sample(node_dist);
                    let node_id = node_ids[node_idx];

                    let _node = api.obj_get(node_id).await.unwrap();
                    node_queries += 1;
                } else {
                    // Arc query
                    let source_idx = rng.sample(node_dist);
                    let target_idx = rng.sample(node_dist);
                    let source_id = node_ids[source_idx];
                    let target_id = node_ids[target_idx];

                    // Select random arc type
                    let arc_type_idx = rng.sample(arc_type_dist);
                    let arc_type = ARC_TYPES[arc_type_idx];

                    let _arc = api.assoc_get(source_id, arc_type, target_id).await.unwrap();
                    arc_queries += 1;
                }

                total_queries += 1;

                // Occasionally perform an update or delete operation (10% of the time)
                if rng.gen_bool(0.1) {
                    let update_type = rng.gen_bool(0.5);

                    if update_type {
                        // Update a node
                        let node_idx = rng.sample(node_dist);
                        let node_id = node_ids[node_idx];

                        // Generate new random data
                        let data_size = rng.gen_range(64..=256);
                        let data = generate_random_data(data_size);

                        let _ = api.obj_update(node_id, data).await.unwrap();
                    } else {
                        // Update an arc
                        let source_idx = rng.sample(node_dist);
                        let target_idx = rng.sample(node_dist);
                        let source_id = node_ids[source_idx];
                        let target_id = node_ids[target_idx];

                        // Select random arc type
                        let arc_type_idx = rng.sample(arc_type_dist);
                        let arc_type = ARC_TYPES[arc_type_idx];

                        // Generate new random data
                        let data_size = rng.gen_range(32..=128);
                        let data = generate_random_data(data_size);

                        let _ = api.assoc_update(source_id, arc_type, target_id, data).await;
                    }

                    total_queries += 1;
                }
            }

            let query_time = query_start_time.elapsed();
            let queries_per_second = total_queries as f64 / query_time.as_secs_f64();

            println!("\nStress Test Results:");
            println!("--------------------");
            println!("Total queries executed: {}", total_queries);
            println!("Node queries: {}", node_queries);
            println!("Arc queries: {}", arc_queries);
            println!("Test duration: {:.2?}", query_time);
            println!("Operations per second: {:.2}", queries_per_second);

            // Clean up
            server.shutdown().await.unwrap();

            println!("Stress test completed successfully.");
        });
    }

    #[test]
    #[ignore] // Ignore by default as it's a long-running test
    fn stress_test_recovery() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Starting recovery stress test...");

            // Create a temporary directory for the database
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("recovery_stress_test.db");

            // Create a server with a larger cache size for better performance
            let config = ServerConfig {
                db_path: db_path.clone(),
                page_size: DEFAULT_PAGE_SIZE,
                cache_size: NonZeroUsize::new(10000).unwrap(), // Larger cache for stress test
                max_disk_space: None, // Unlimited for stress test
                flush_interval_ms: 1000, // 1 second flush interval
            };

            // First server instance
            let server1 = Server::with_config(config.clone()).await.unwrap();
            let api1 = YarnCacheApi::new(Arc::new(server1.clone()));

            println!("Creating {} nodes and {} arcs...", NUM_NODES, NUM_ARCS);

            // Create nodes
            let node_ids: Vec<u64> = (1..=NUM_NODES as u64).collect();
            let mut rng = thread_rng();

            for &node_id in &node_ids {
                // Generate random data between 64 and 256 bytes
                let data_size = rng.gen_range(64..=256);
                let data = generate_random_data(data_size);

                api1.obj_add(node_id, NODE_TYPE, data).await.unwrap();
            }

            // Create arcs
            println!("Creating arcs...");
            let mut arc_count = 0;

            // Distribution for selecting random nodes
            let node_dist = Uniform::from(0..node_ids.len());
            // Distribution for selecting random arc types
            let arc_type_dist = Uniform::from(0..ARC_TYPES.len());

            for _ in 0..NUM_ARCS {
                // Select random source and target nodes
                let source_idx = rng.sample(node_dist);
                let target_idx = rng.sample(node_dist);
                let source_id = node_ids[source_idx];
                let target_id = node_ids[target_idx];

                // Select random arc type
                let arc_type_idx = rng.sample(arc_type_dist);
                let arc_type = ARC_TYPES[arc_type_idx];

                // Generate random timestamp
                let timestamp = rng.gen_range(1..=100000);

                // Generate random data between 32 and 128 bytes
                let data_size = rng.gen_range(32..=128);
                let data = generate_random_data(data_size);

                // Create the arc
                api1.assoc_add(source_id, arc_type, target_id, timestamp, data).await.unwrap();
                arc_count += 1;
            }

            println!("Created {} nodes and {} arcs", NUM_NODES, arc_count);

            // Shutdown the first server
            println!("Shutting down server...");
            server1.shutdown().await.unwrap();

            // Create a second server instance with the same database
            println!("Starting new server and recovering data...");
            let server2 = Server::with_config(config).await.unwrap();
            let api2 = YarnCacheApi::new(Arc::new(server2.clone()));

            // Recover the database
            let recovery_start = Instant::now();
            api2.recover().await.unwrap();
            let recovery_time = recovery_start.elapsed();

            println!("Recovery completed in {:.2?}", recovery_time);

            // Verify a sample of nodes
            println!("Verifying node recovery...");
            let verification_start = Instant::now();

            // Verify 10% of nodes
            let node_sample_size = NUM_NODES / 10;
            let node_sample: Vec<u64> = node_ids.choose_multiple(&mut rng, node_sample_size).cloned().collect();

            let mut nodes_found = 0;
            for &node_id in &node_sample {
                let node = api2.obj_get(node_id).await.unwrap();
                if node.is_some() {
                    nodes_found += 1;
                }
            }

            println!("Found {}/{} sampled nodes after recovery", nodes_found, node_sample_size);
            assert!(nodes_found > node_sample_size * 9 / 10, "Too many nodes missing after recovery");

            let verification_time = verification_start.elapsed();
            println!("Node verification completed in {:.2?}", verification_time);

            println!("Recovery stress test completed successfully.");

            // Clean up
            server2.shutdown().await.unwrap();
        });
    }
}
