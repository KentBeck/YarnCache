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

    /// Create an association
    pub fn assoc_add(&self, id1: u64, atype: u64, id2: u64, time: u64, data: Vec<u8>) -> Result<GraphArc> {
        // Check if both nodes exist
        let nodes = self.nodes.lock().unwrap();
        if !nodes.contains_key(&id1) {
            return Err(Error::NotFound(format!("Source node with ID {} not found", id1)));
        }
        if !nodes.contains_key(&id2) {
            return Err(Error::NotFound(format!("Target node with ID {} not found", id2)));
        }
        drop(nodes);

        // Create a new arc
        let arc_id = ArcId(id1 ^ id2 ^ atype); // Simple hash for demo purposes
        let from_node = NodeId(id1);
        let to_node = NodeId(id2);
        let type_id = TypeId(atype);
        let timestamp = Timestamp(time);

        let arc = GraphArc {
            id: arc_id,
            timestamp,
            type_id,
            from_node,
            to_node,
            data,
        };

        // Store the arc
        let mut arcs = self.arcs.lock().unwrap();
        arcs.insert(arc_id.0, arc.clone());

        // Update indices
        let mut arcs_by_source = self.arcs_by_source.lock().unwrap();
        arcs_by_source.entry((id1, atype)).or_insert_with(Vec::new).push(arc_id.0);

        let mut arcs_by_target = self.arcs_by_target.lock().unwrap();
        arcs_by_target.entry((id2, atype)).or_insert_with(Vec::new).push(arc_id.0);

        let mut arcs_by_time = self.arcs_by_time.lock().unwrap();
        arcs_by_time.entry(time).or_insert_with(Vec::new).push(arc_id.0);

        Ok(arc)
    }

    /// Delete an association
    pub fn assoc_delete(&self, id1: u64, atype: u64, id2: u64) -> Result<bool> {
        // Find the arc ID
        let arc_id = id1 ^ id2 ^ atype; // Same hash as in assoc_add

        // Remove from the arcs map
        let mut arcs = self.arcs.lock().unwrap();
        let arc = match arcs.remove(&arc_id) {
            Some(arc) => arc,
            None => return Ok(false),
        };

        // Remove from indices
        let mut arcs_by_source = self.arcs_by_source.lock().unwrap();
        if let Some(ids) = arcs_by_source.get_mut(&(id1, atype)) {
            ids.retain(|&id| id != arc_id);
        }

        let mut arcs_by_target = self.arcs_by_target.lock().unwrap();
        if let Some(ids) = arcs_by_target.get_mut(&(id2, atype)) {
            ids.retain(|&id| id != arc_id);
        }

        let mut arcs_by_time = self.arcs_by_time.lock().unwrap();
        if let Some(ids) = arcs_by_time.get_mut(&arc.timestamp.0) {
            ids.retain(|&id| id != arc_id);
        }

        Ok(true)
    }

    /// Get a specific association
    pub fn assoc_get(&self, id1: u64, atype: u64, id2: u64) -> Result<Option<GraphArc>> {
        // Find the arc ID
        let arc_id = id1 ^ id2 ^ atype; // Same hash as in assoc_add

        // Get the arc
        let arcs = self.arcs.lock().unwrap();
        Ok(arcs.get(&arc_id).cloned())
    }

    /// Update association data
    pub fn assoc_update(&self, id1: u64, atype: u64, id2: u64, data: Vec<u8>) -> Result<GraphArc> {
        // Find the arc ID
        let arc_id = id1 ^ id2 ^ atype; // Same hash as in assoc_add

        // Get the arc
        let mut arcs = self.arcs.lock().unwrap();
        let arc = match arcs.get(&arc_id) {
            Some(arc) => arc.clone(),
            None => return Err(Error::NotFound(format!("Association not found"))),
        };

        // Create an updated arc
        let updated_arc = GraphArc {
            id: arc.id,
            timestamp: arc.timestamp,
            type_id: arc.type_id,
            from_node: arc.from_node,
            to_node: arc.to_node,
            data,
        };

        // Store the updated arc
        arcs.insert(arc_id, updated_arc.clone());

        Ok(updated_arc)
    }

    /// Get associations in a range
    pub fn assoc_get_range(&self, id1: u64, atype: u64, id2_lbound: u64, id2_ubound: u64, limit: usize) -> Result<Vec<GraphArc>> {
        let mut result = Vec::new();

        // Get all arcs for this source and type
        let arcs_by_source = self.arcs_by_source.lock().unwrap();
        let arc_ids = match arcs_by_source.get(&(id1, atype)) {
            Some(ids) => ids.clone(),
            None => return Ok(Vec::new()),
        };
        drop(arcs_by_source);

        // Get all arcs
        let arcs = self.arcs.lock().unwrap();

        // Filter by target ID range and collect up to limit
        for arc_id in arc_ids {
            if let Some(arc) = arcs.get(&arc_id) {
                let target_id = arc.to_node.0;
                if target_id >= id2_lbound && target_id <= id2_ubound {
                    result.push(arc.clone());
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Count associations in a range
    pub fn assoc_count_range(&self, id1: u64, atype: u64, id2_lbound: u64, id2_ubound: u64) -> Result<usize> {
        let mut count = 0;

        // Get all arcs for this source and type
        let arcs_by_source = self.arcs_by_source.lock().unwrap();
        let arc_ids = match arcs_by_source.get(&(id1, atype)) {
            Some(ids) => ids.clone(),
            None => return Ok(0),
        };
        drop(arcs_by_source);

        // Get all arcs
        let arcs = self.arcs.lock().unwrap();

        // Filter by target ID range and count
        for arc_id in arc_ids {
            if let Some(arc) = arcs.get(&arc_id) {
                let target_id = arc.to_node.0;
                if target_id >= id2_lbound && target_id <= id2_ubound {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Get associations in a time range
    pub fn assoc_time_range(&self, id1: u64, atype: u64, time_lbound: u64, time_ubound: u64, limit: usize) -> Result<Vec<GraphArc>> {
        let mut result = Vec::new();

        // Get all arcs for this source and type
        let arcs_by_source = self.arcs_by_source.lock().unwrap();
        let arc_ids = match arcs_by_source.get(&(id1, atype)) {
            Some(ids) => ids.clone(),
            None => return Ok(Vec::new()),
        };
        drop(arcs_by_source);

        // Get all arcs
        let arcs = self.arcs.lock().unwrap();

        // Filter by time range and collect up to limit
        for arc_id in arc_ids {
            if let Some(arc) = arcs.get(&arc_id) {
                let time = arc.timestamp.0;
                if time >= time_lbound && time <= time_ubound {
                    result.push(arc.clone());
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get multiple objects
    pub fn obj_get_multi(&self, ids: Vec<u64>) -> Result<Vec<Node>> {
        let mut result = Vec::new();
        let nodes = self.nodes.lock().unwrap();

        for id in ids {
            if let Some(node) = nodes.get(&id) {
                result.push(node.clone());
            }
        }

        Ok(result)
    }

    /// Get multiple associations
    pub fn assoc_get_multi(&self, id1s: Vec<u64>, atype: u64, id2s: Vec<u64>) -> Result<Vec<GraphArc>> {
        let mut result = Vec::new();
        let arcs = self.arcs.lock().unwrap();

        for id1 in &id1s {
            for id2 in &id2s {
                let arc_id = id1 ^ id2 ^ atype; // Same hash as in assoc_add
                if let Some(arc) = arcs.get(&arc_id) {
                    result.push(arc.clone());
                }
            }
        }

        Ok(result)
    }

    /// Count associations for multiple objects
    pub fn assoc_count_multi(&self, id1s: Vec<u64>, atype: u64) -> Result<HashMap<u64, usize>> {
        let mut result = HashMap::new();
        let arcs_by_source = self.arcs_by_source.lock().unwrap();

        for id1 in id1s {
            let count = match arcs_by_source.get(&(id1, atype)) {
                Some(ids) => ids.len(),
                None => 0,
            };
            result.insert(id1, count);
        }

        Ok(result)
    }

    /// Get inverse associations
    pub fn assoc_get_inverse(&self, id2: u64, atype: u64, id1_lbound: u64, id1_ubound: u64, limit: usize) -> Result<Vec<GraphArc>> {
        let mut result = Vec::new();

        // Get all arcs for this target and type
        let arcs_by_target = self.arcs_by_target.lock().unwrap();
        let arc_ids = match arcs_by_target.get(&(id2, atype)) {
            Some(ids) => ids.clone(),
            None => return Ok(Vec::new()),
        };
        drop(arcs_by_target);

        // Get all arcs
        let arcs = self.arcs.lock().unwrap();

        // Filter by source ID range and collect up to limit
        for arc_id in arc_ids {
            if let Some(arc) = arcs.get(&arc_id) {
                let source_id = arc.from_node.0;
                if source_id >= id1_lbound && source_id <= id1_ubound {
                    result.push(arc.clone());
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get objects connected via two association types
    pub fn assoc_get_both(&self, id1: u64, atype1: u64, atype2: u64) -> Result<Vec<(u64, Vec<GraphArc>)>> {
        let mut result = Vec::new();

        // Get all arcs for the first type
        let arcs_by_source = self.arcs_by_source.lock().unwrap();
        let arc_ids1 = match arcs_by_source.get(&(id1, atype1)) {
            Some(ids) => ids.clone(),
            None => return Ok(Vec::new()),
        };
        drop(arcs_by_source);

        // Get all arcs
        let arcs = self.arcs.lock().unwrap();

        // For each target of the first type, find all its outgoing arcs of the second type
        for arc_id1 in arc_ids1 {
            if let Some(arc1) = arcs.get(&arc_id1) {
                let intermediate_id = arc1.to_node.0;

                // Get all arcs for the intermediate node and second type
                let arcs_by_source = self.arcs_by_source.lock().unwrap();
                let arc_ids2 = match arcs_by_source.get(&(intermediate_id, atype2)) {
                    Some(ids) => ids.clone(),
                    None => continue,
                };
                drop(arcs_by_source);

                // Collect all second-level arcs
                let mut second_level_arcs = Vec::new();
                for arc_id2 in arc_ids2 {
                    if let Some(arc2) = arcs.get(&arc_id2) {
                        second_level_arcs.push(arc2.clone());
                    }
                }

                if !second_level_arcs.is_empty() {
                    result.push((intermediate_id, second_level_arcs));
                }
            }
        }

        Ok(result)
    }

    /// Count objects connected via two association types
    pub fn assoc_count_both(&self, id1: u64, atype1: u64, atype2: u64) -> Result<HashMap<u64, usize>> {
        let mut result = HashMap::new();

        // Get all arcs for the first type
        let arcs_by_source = self.arcs_by_source.lock().unwrap();
        let arc_ids1 = match arcs_by_source.get(&(id1, atype1)) {
            Some(ids) => ids.clone(),
            None => return Ok(HashMap::new()),
        };
        drop(arcs_by_source);

        // Get all arcs
        let arcs = self.arcs.lock().unwrap();

        // For each target of the first type, count all its outgoing arcs of the second type
        for arc_id1 in arc_ids1 {
            if let Some(arc1) = arcs.get(&arc_id1) {
                let intermediate_id = arc1.to_node.0;

                // Get all arcs for the intermediate node and second type
                let arcs_by_source = self.arcs_by_source.lock().unwrap();
                let count = match arcs_by_source.get(&(intermediate_id, atype2)) {
                    Some(ids) => ids.len(),
                    None => 0,
                };
                drop(arcs_by_source);

                if count > 0 {
                    result.insert(intermediate_id, count);
                }
            }
        }

        Ok(result)
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

    #[test]
    fn test_association_operations() {
        let server = ReferenceServer::new();

        // Create two nodes
        let id1 = 1;
        let id2 = 2;
        let type_id = 10;
        server.obj_add(id1, type_id, vec![1, 2, 3]).unwrap();
        server.obj_add(id2, type_id, vec![4, 5, 6]).unwrap();

        // Test assoc_add
        let atype = 100;
        let time = 12345;
        let data = vec![7, 8, 9];
        let arc = server.assoc_add(id1, atype, id2, time, data.clone()).unwrap();

        assert_eq!(arc.from_node.0, id1);
        assert_eq!(arc.to_node.0, id2);
        assert_eq!(arc.type_id.0, atype);
        assert_eq!(arc.timestamp.0, time);
        assert_eq!(arc.data, data);

        // Test assoc_get
        let retrieved_arc = server.assoc_get(id1, atype, id2).unwrap().unwrap();
        assert_eq!(retrieved_arc.from_node.0, id1);
        assert_eq!(retrieved_arc.to_node.0, id2);
        assert_eq!(retrieved_arc.type_id.0, atype);
        assert_eq!(retrieved_arc.data, data);

        // Test assoc_update
        let new_data = vec![10, 11, 12];
        let updated_arc = server.assoc_update(id1, atype, id2, new_data.clone()).unwrap();
        assert_eq!(updated_arc.from_node.0, id1);
        assert_eq!(updated_arc.to_node.0, id2);
        assert_eq!(updated_arc.type_id.0, atype);
        assert_eq!(updated_arc.data, new_data);

        // Verify the update
        let retrieved_arc = server.assoc_get(id1, atype, id2).unwrap().unwrap();
        assert_eq!(retrieved_arc.data, new_data);

        // Test assoc_delete
        let deleted = server.assoc_delete(id1, atype, id2).unwrap();
        assert!(deleted);

        // Verify the deletion
        let retrieved_arc = server.assoc_get(id1, atype, id2).unwrap();
        assert!(retrieved_arc.is_none());

        // Test deleting a non-existent association
        let deleted = server.assoc_delete(id1, atype, id2).unwrap();
        assert!(!deleted);

        // Test error cases

        // Try to create an association with non-existent source node
        let result = server.assoc_add(999, atype, id2, time, data.clone());
        assert!(result.is_err());

        // Try to create an association with non-existent target node
        let result = server.assoc_add(id1, atype, 999, time, data.clone());
        assert!(result.is_err());

        // Try to update a non-existent association
        let result = server.assoc_update(id1, 999, id2, data.clone());
        assert!(result.is_err());
    }

    #[test]
    fn test_range_query_operations() {
        let server = ReferenceServer::new();

        // Create nodes
        let source_id = 1;
        let target_ids = vec![10, 20, 30, 40, 50];
        let node_type = 100;
        let arc_type = 200;

        // Add the source node
        server.obj_add(source_id, node_type, vec![1]).unwrap();

        // Add target nodes and create associations with different timestamps
        for (i, &target_id) in target_ids.iter().enumerate() {
            // Add target node
            server.obj_add(target_id, node_type, vec![i as u8 + 1]).unwrap();

            // Create association with timestamp = target_id * 100
            let time = target_id * 100;
            server.assoc_add(source_id, arc_type, target_id, time, vec![i as u8]).unwrap();
        }

        // Test assoc_get_range
        let arcs = server.assoc_get_range(source_id, arc_type, 20, 40, 10).unwrap();
        assert_eq!(arcs.len(), 3);
        assert!(arcs.iter().any(|a| a.to_node.0 == 20));
        assert!(arcs.iter().any(|a| a.to_node.0 == 30));
        assert!(arcs.iter().any(|a| a.to_node.0 == 40));

        // Test assoc_get_range with limit
        let arcs = server.assoc_get_range(source_id, arc_type, 20, 40, 2).unwrap();
        assert_eq!(arcs.len(), 2);

        // Test assoc_count_range
        let count = server.assoc_count_range(source_id, arc_type, 20, 40).unwrap();
        assert_eq!(count, 3);

        // Test assoc_time_range
        let arcs = server.assoc_time_range(source_id, arc_type, 2000, 4000, 10).unwrap();
        assert_eq!(arcs.len(), 3);
        assert!(arcs.iter().any(|a| a.timestamp.0 == 2000));
        assert!(arcs.iter().any(|a| a.timestamp.0 == 3000));
        assert!(arcs.iter().any(|a| a.timestamp.0 == 4000));

        // Test assoc_time_range with limit
        let arcs = server.assoc_time_range(source_id, arc_type, 2000, 4000, 2).unwrap();
        assert_eq!(arcs.len(), 2);

        // Test with non-existent source
        let arcs = server.assoc_get_range(999, arc_type, 0, 100, 10).unwrap();
        assert_eq!(arcs.len(), 0);

        let count = server.assoc_count_range(999, arc_type, 0, 100).unwrap();
        assert_eq!(count, 0);

        let arcs = server.assoc_time_range(999, arc_type, 0, 10000, 10).unwrap();
        assert_eq!(arcs.len(), 0);

        // Test with non-existent arc type
        let arcs = server.assoc_get_range(source_id, 999, 0, 100, 10).unwrap();
        assert_eq!(arcs.len(), 0);

        let count = server.assoc_count_range(source_id, 999, 0, 100).unwrap();
        assert_eq!(count, 0);

        let arcs = server.assoc_time_range(source_id, 999, 0, 10000, 10).unwrap();
        assert_eq!(arcs.len(), 0);

        // Test with empty range
        let arcs = server.assoc_get_range(source_id, arc_type, 60, 70, 10).unwrap();
        assert_eq!(arcs.len(), 0);

        let count = server.assoc_count_range(source_id, arc_type, 60, 70).unwrap();
        assert_eq!(count, 0);

        let arcs = server.assoc_time_range(source_id, arc_type, 6000, 7000, 10).unwrap();
        assert_eq!(arcs.len(), 0);
    }

    #[test]
    fn test_batch_operations() {
        let server = ReferenceServer::new();

        // Create multiple nodes
        let node_ids = vec![1, 2, 3, 4, 5];
        let node_type = 100;

        for id in &node_ids {
            server.obj_add(*id, node_type, vec![*id as u8]).unwrap();
        }

        // Create associations between nodes
        let arc_type = 200;
        let source_ids = vec![1, 2];
        let target_ids = vec![3, 4, 5];

        for &source_id in &source_ids {
            for &target_id in &target_ids {
                server.assoc_add(source_id, arc_type, target_id, 1000, vec![]).unwrap();
            }
        }

        // Test obj_get_multi
        let nodes = server.obj_get_multi(vec![1, 3, 5]).unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.iter().any(|n| n.id.0 == 1));
        assert!(nodes.iter().any(|n| n.id.0 == 3));
        assert!(nodes.iter().any(|n| n.id.0 == 5));

        // Test obj_get_multi with non-existent nodes
        let nodes = server.obj_get_multi(vec![1, 999]).unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes.iter().any(|n| n.id.0 == 1));

        // Test assoc_get_multi
        let arcs = server.assoc_get_multi(vec![1, 2], arc_type, vec![3, 4]).unwrap();
        assert_eq!(arcs.len(), 4); // 2 sources * 2 targets

        // Test assoc_get_multi with non-existent associations
        let arcs = server.assoc_get_multi(vec![1, 999], arc_type, vec![3, 999]).unwrap();
        assert_eq!(arcs.len(), 1); // Only (1,3) exists

        // Test assoc_count_multi
        let counts = server.assoc_count_multi(vec![1, 2, 999], arc_type).unwrap();
        assert_eq!(counts.len(), 3);
        assert_eq!(counts.get(&1), Some(&3)); // Node 1 has 3 associations
        assert_eq!(counts.get(&2), Some(&3)); // Node 2 has 3 associations
        assert_eq!(counts.get(&999), Some(&0)); // Node 999 has 0 associations
    }
}
