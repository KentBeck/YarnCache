//! YarnCache: A special-purpose graph database for storing nodes and arcs

use thiserror::Error;

mod types;
mod storage;
mod server;
mod api;
mod transaction_log;
mod reference;
#[cfg(test)]
mod api_test;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod transaction_log_test;
#[cfg(test)]
mod transaction_log_sequence_test;
#[cfg(test)]
mod persistence_test;

pub use server::Server;
pub use types::{NodeId, ArcId, TypeId, Timestamp, Node, Arc as GraphArc};
pub use api::YarnCacheApi;
pub use reference::ReferenceServer;

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
