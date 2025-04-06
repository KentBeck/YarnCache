//! API for the YarnCache database
//!
//! This module provides the public API for interacting with the YarnCache database.
//! It includes operations for objects (nodes) and associations (arcs).

use std::sync::Arc as StdArc;

use crate::{Result, Error, NodeId, TypeId, Node};
use crate::server::Server;

/// API for the YarnCache database
pub struct YarnCacheApi {
    /// The server instance
    server: StdArc<Server>,
}

impl YarnCacheApi {
    /// Create a new API instance
    pub fn new(server: StdArc<Server>) -> Self {
        Self { server }
    }

    // Object Operations

    /// Create a new object
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the object
    /// * `type_id` - The type of the object
    /// * `data` - The data associated with the object
    ///
    /// # Returns
    ///
    /// The created object
    pub async fn obj_add(&self, id: u64, type_id: u64, data: Vec<u8>) -> Result<Node> {
        let node_id = NodeId(id);
        let type_id = TypeId(type_id);
        let node = Node::new(node_id, type_id, data);

        // Store the node
        self.server.storage().store_node(&node)?;

        Ok(node)
    }

    /// Retrieve an object by ID
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the object
    ///
    /// # Returns
    ///
    /// The object, if found
    pub async fn obj_get(&self, id: u64) -> Result<Option<Node>> {
        let node_id = NodeId(id);
        self.server.storage().get_node(node_id)
    }

    /// Update an object's data
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the object
    /// * `data` - The new data for the object
    ///
    /// # Returns
    ///
    /// The updated object
    pub async fn obj_update(&self, id: u64, data: Vec<u8>) -> Result<Node> {
        let node_id = NodeId(id);

        // Get the existing node
        let node = match self.server.storage().get_node(node_id)? {
            Some(node) => node,
            None => return Err(Error::NotFound(format!("Node with ID {} not found", id))),
        };

        // Create an updated node with the same ID and type, but new data
        let updated_node = Node {
            id: node.id,
            timestamp: node.timestamp,
            type_id: node.type_id,
            data,
        };

        // Store the updated node
        self.server.storage().store_node(&updated_node)?;

        Ok(updated_node)
    }

    /// Delete an object
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the object
    ///
    /// # Returns
    ///
    /// `true` if the object was deleted, `false` if it didn't exist
    pub async fn obj_delete(&self, id: u64) -> Result<bool> {
        let node_id = NodeId(id);
        self.server.storage().delete_node(node_id)
    }
}
