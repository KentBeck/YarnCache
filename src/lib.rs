//! YarnCache: A special-purpose graph database for storing nodes and arcs

use thiserror::Error;

mod api;
#[cfg(test)]
mod api_test;
#[cfg(test)]
mod disk_space_test;
#[cfg(test)]
mod persistence_test;
#[cfg(test)]
mod recovery_test;
mod reference;
mod server;
mod storage;
mod transaction_log;
#[cfg(test)]
mod transaction_log_sequence_test;
#[cfg(test)]
mod transaction_log_test;
mod types;

pub use api::YarnCacheApi;
pub use reference::ReferenceServer;
pub use server::Server;
pub use types::{Arc as GraphArc, ArcId, Node, NodeId, Timestamp, TypeId};

/// Error types for the YarnCache database
#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Data corruption detected: {0}")]
    Corruption(String),

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Disk space exceeded: {0}")]
    DiskSpaceExceeded(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_server_start_stop() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = Server::new().await.unwrap();
            assert!(server.is_running());
            server.shutdown().await.unwrap();
            // Note: we don't check is_running() after shutdown because it should be false
            // but we don't consume the server now
        });
    }
}
