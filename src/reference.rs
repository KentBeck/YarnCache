//! Reference implementation of the YarnCache API
//!
//! This module provides an in-memory reference implementation of the YarnCache API
//! for testing and specification purposes.

use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, Mutex};

use crate::{Result, Error, Node, NodeId, TypeId, GraphArc, ArcId, Timestamp};

/// In-memory reference implementation of the YarnCache API
pub struct ReferenceServer {
    /// Nodes stored by ID
    nodes: Arc<Mutex<HashMap<u64, Node>>>,
    /// Arcs stored by ID
    arcs: Arc<Mutex<HashMap<u64, GraphArc>>>,
    /// Arcs indexed by source node ID and type
    arcs_by_source: Arc<Mutex<HashMap<(u64, u64), Vec<u64>>>>,
    /// Arcs indexed by target node ID and type
    arcs_by_target: Arc<Mutex<HashMap<(u64, u64), Vec<u64>>>>,
    /// Arcs indexed by timestamp
    arcs_by_time: Arc<Mutex<BTreeMap<u64, Vec<u64>>>>,
}

impl ReferenceServer {
    /// Create a new reference server
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            arcs: Arc::new(Mutex::new(HashMap::new())),
            arcs_by_source: Arc::new(Mutex::new(HashMap::new())),
            arcs_by_target: Arc::new(Mutex::new(HashMap::new())),
            arcs_by_time: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Create a new object
    pub fn obj_add(&self, id: u64, type_id: u64, data: Vec<u8>) -> Result<Node> {
        let node_id = NodeId(id);
        let type_id = TypeId(type_id);
        let node = Node::new(node_id, type_id, data);
        
        // Store the node
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(id, node.clone());
        
        Ok(node)
    }

    /// Retrieve an object by ID
    pub fn obj_get(&self, id: u64) -> Result<Option<Node>> {
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes.get(&id).cloned())
    }

    /// Update an object's data
    pub fn obj_update(&self, id: u64, data: Vec<u8>) -> Result<Node> {
        let mut nodes = self.nodes.lock().unwrap();
        
        // Get the existing node
        let node = match nodes.get(&id) {
            Some(node) => node.clone(),
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
        nodes.insert(id, updated_node.clone());
        
        Ok(updated_node)
    }

    /// Delete an object
    pub fn obj_delete(&self, id: u64) -> Result<bool> {
        let mut nodes = self.nodes.lock().unwrap();
        Ok(nodes.remove(&id).is_some())
    }
}

impl Default for ReferenceServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_operations() {
        let server = ReferenceServer::new();
        
        // Test obj_add
        let id = 1;
        let type_id = 2;
        let data = vec![1, 2, 3, 4];
        let node = server.obj_add(id, type_id, data.clone()).unwrap();
        
        assert_eq!(node.id.0, id);
        assert_eq!(node.type_id.0, type_id);
        assert_eq!(node.data, data);
        
        // Test obj_get
        let retrieved_node = server.obj_get(id).unwrap().unwrap();
        assert_eq!(retrieved_node.id.0, id);
        assert_eq!(retrieved_node.type_id.0, type_id);
        assert_eq!(retrieved_node.data, data);
        
        // Test obj_update
        let new_data = vec![5, 6, 7, 8];
        let updated_node = server.obj_update(id, new_data.clone()).unwrap();
        assert_eq!(updated_node.id.0, id);
        assert_eq!(updated_node.type_id.0, type_id);
        assert_eq!(updated_node.data, new_data);
        
        // Verify the update
        let retrieved_node = server.obj_get(id).unwrap().unwrap();
        assert_eq!(retrieved_node.data, new_data);
        
        // Test obj_delete
        let deleted = server.obj_delete(id).unwrap();
        assert!(deleted);
        
        // Verify the deletion
        let retrieved_node = server.obj_get(id).unwrap();
        assert!(retrieved_node.is_none());
        
        // Test deleting a non-existent node
        let deleted = server.obj_delete(999).unwrap();
        assert!(!deleted);
    }
}
