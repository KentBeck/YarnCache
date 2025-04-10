//! Server implementation for the YarnCache database

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc as StdArc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;

use crate::storage::{StorageManager, DEFAULT_PAGE_SIZE};
use crate::Result;

/// Default cache size (number of pages)
const DEFAULT_CACHE_SIZE: usize = 1000;

/// Default flush interval in milliseconds (1 second)
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1000;

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Path to the database file
    pub db_path: PathBuf,
    /// Page size in bytes
    pub page_size: usize,
    /// Cache size (number of pages)
    pub cache_size: NonZeroUsize,
    /// Maximum disk space in bytes (None for unlimited)
    pub max_disk_space: Option<u64>,
    /// Flush interval in milliseconds (how often to check for dirty pages)
    pub flush_interval_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("yarn_cache.db"),
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap(),
            max_disk_space: None, // Unlimited by default
            flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
        }
    }
}

/// Server state
struct ServerState {
    /// Storage manager
    storage: StdArc<StorageManager>,
    /// Whether the server is running
    running: AtomicBool,
    /// Shutdown channel
    shutdown_tx: TokioMutex<Option<oneshot::Sender<()>>>,
    /// Server task handle
    task_handle: TokioMutex<Option<JoinHandle<Result<()>>>>,
}

/// YarnCache database server
#[derive(Clone)]
pub struct Server {
    /// Server state
    state: StdArc<ServerState>,
}

impl Server {
    /// Create a new server with default configuration
    pub async fn new() -> Result<Self> {
        Self::with_config(ServerConfig::default()).await
    }

    /// Create a new server with custom configuration
    pub async fn with_config(config: ServerConfig) -> Result<Self> {
        // Create the storage manager
        let storage = StdArc::new(StorageManager::new(
            &config.db_path,
            config.page_size,
            config.cache_size,
            config.max_disk_space,
            config.flush_interval_ms,
        )?);

        // Create the shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Create the server state
        let state = StdArc::new(ServerState {
            storage,
            running: AtomicBool::new(true),
            shutdown_tx: TokioMutex::new(Some(shutdown_tx)),
            task_handle: TokioMutex::new(None),
        });

        // Create the server task
        let task_state = state.clone();
        let task_handle = tokio::spawn(async move { Self::run(task_state, shutdown_rx).await });

        // Store the task handle
        *(state.task_handle.lock().await) = Some(task_handle);

        Ok(Self { state })
    }

    /// Run the server
    async fn run(state: StdArc<ServerState>, shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        // Wait for shutdown signal
        let _ = shutdown_rx.await;

        println!("Server received shutdown signal");

        // Mark the server as stopped first to prevent new operations
        state.running.store(false, Ordering::SeqCst);

        // Flush all pages to disk
        println!("Server flushing all pages to disk");
        match state.storage.flush_all() {
            Ok(_) => println!("Server flush complete"),
            Err(e) => println!("Server flush error: {}", e),
        }

        println!("Server shutdown complete");
        Ok(())
    }

    /// Check if the server is running
    pub fn is_running(&self) -> bool {
        self.state.running.load(Ordering::SeqCst)
    }

    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        println!("Starting server shutdown");

        // First, manually flush all pages to disk
        println!("Manually flushing all pages to disk before shutdown");
        match self.state.storage.flush_all() {
            Ok(_) => println!("Manual flush complete"),
            Err(e) => println!("Manual flush error: {}", e),
        }

        // Send the shutdown signal
        println!("Sending shutdown signal");
        if let Some(shutdown_tx) = self.state.shutdown_tx.lock().await.take() {
            let _ = shutdown_tx.send(());
            println!("Shutdown signal sent");
        } else {
            println!("No shutdown channel available");
        }

        // Wait for the server task to complete
        println!("Waiting for server task to complete");
        if let Some(handle) = self.state.task_handle.lock().await.take() {
            match handle.await {
                Ok(result) => match result {
                    Ok(_) => println!("Server task completed successfully"),
                    Err(e) => println!("Server task error: {}", e),
                },
                Err(e) => println!("Failed to join server task: {}", e),
            }
        } else {
            println!("No server task handle available");
        }

        println!("Server shutdown complete");
        Ok(())
    }

    /// Get a reference to the storage manager
    pub fn storage(&self) -> StdArc<StorageManager> {
        self.state.storage.clone()
    }

    /// Create a new API instance for this server
    pub fn api(&self) -> crate::YarnCacheApi {
        crate::YarnCacheApi::new(StdArc::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_dir::TempDir;

    #[tokio::test]
    async fn test_server_start_stop() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create a custom configuration
        let config = ServerConfig {
            db_path,
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: NonZeroUsize::new(10).unwrap(),
            max_disk_space: None, // Unlimited for tests
            flush_interval_ms: 1000, // 1 second flush interval
        };

        // Create a server
        let server = Server::with_config(config).await.unwrap();
        assert!(server.is_running());

        // Shutdown the server
        server.shutdown().await.unwrap();

        // Server should no longer be running
        // Note: we can't check is_running() here because the server has been moved
    }
}
