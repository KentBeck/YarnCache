//! API for the YarnCache database
//!
//! This module provides the public API for interacting with the YarnCache database.
//! It includes operations for objects (nodes) and associations (arcs).

use std::sync::Arc as StdArc;

use crate::server::Server;
use crate::{ArcId, Error, GraphArc, Node, NodeId, Result, Timestamp, TypeId};

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
        self.server.storage().update_node(&updated_node)?;

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

    /// Recover the database from the transaction log
    ///
    /// This method replays the transaction log to recover the database state
    /// after a crash or unexpected shutdown.
    pub async fn recover(&self) -> Result<()> {
        self.server.storage().recover()
    }

    // Association Operations

    /// Create an association
    ///
    /// # Arguments
    ///
    /// * `id1` - The ID of the source object
    /// * `atype` - The type of the association
    /// * `id2` - The ID of the target object
    /// * `time` - The timestamp of the association
    /// * `data` - The data associated with the association
    ///
    /// # Returns
    ///
    /// The created association
    pub async fn assoc_add(
        &self,
        id1: u64,
        atype: u64,
        id2: u64,
        time: u64,
        data: Vec<u8>,
    ) -> Result<GraphArc> {
        let from_node = NodeId(id1);
        let to_node = NodeId(id2);
        let type_id = TypeId(atype);
        let timestamp = Timestamp(time);

        // Create a simple hash for the arc ID
        let arc_id = ArcId(id1 ^ id2 ^ atype);

        let arc = GraphArc {
            id: arc_id,
            timestamp,
            type_id,
            from_node,
            to_node,
            data,
        };

        // Store the arc
        self.server.storage().store_arc(&arc)?;

        Ok(arc)
    }

    /// Get a specific association
    ///
    /// # Arguments
    ///
    /// * `id1` - The ID of the source object
    /// * `atype` - The type of the association
    /// * `id2` - The ID of the target object
    ///
    /// # Returns
    ///
    /// The association, if found
    pub async fn assoc_get(&self, id1: u64, atype: u64, id2: u64) -> Result<Option<GraphArc>> {
        let arc_id = ArcId(id1 ^ id2 ^ atype);
        self.server.storage().get_arc(arc_id)
    }

    /// Update an association's data
    ///
    /// # Arguments
    ///
    /// * `id1` - The ID of the source object
    /// * `atype` - The type of the association
    /// * `id2` - The ID of the target object
    /// * `data` - The new data for the association
    ///
    /// # Returns
    ///
    /// The updated association
    pub async fn assoc_update(
        &self,
        id1: u64,
        atype: u64,
        id2: u64,
        data: Vec<u8>,
    ) -> Result<GraphArc> {
        let arc_id = ArcId(id1 ^ id2 ^ atype);

        // Get the existing arc
        let arc = match self.server.storage().get_arc(arc_id)? {
            Some(arc) => arc,
            None => return Err(Error::NotFound(format!("Association not found"))),
        };

        // Create an updated arc with the same IDs and type, but new data
        let updated_arc = GraphArc {
            id: arc.id,
            timestamp: arc.timestamp,
            type_id: arc.type_id,
            from_node: arc.from_node,
            to_node: arc.to_node,
            data,
        };

        // Store the updated arc
        self.server.storage().update_arc(&updated_arc)?;

        Ok(updated_arc)
    }

    /// Delete an association
    ///
    /// # Arguments
    ///
    /// * `id1` - The ID of the source object
    /// * `atype` - The type of the association
    /// * `id2` - The ID of the target object
    ///
    /// # Returns
    ///
    /// `true` if the association was deleted, `false` if it didn't exist
    pub async fn assoc_delete(&self, id1: u64, atype: u64, id2: u64) -> Result<bool> {
        let arc_id = ArcId(id1 ^ id2 ^ atype);
        self.server.storage().delete_arc(arc_id)
    }
}
