//! Core types for the YarnCache database

use serde::{Deserialize, Serialize};
use std::fmt;

/// A 64-bit UUID used to identify nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// A 64-bit UUID used to identify arcs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArcId(pub u64);

impl fmt::Display for ArcId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// A 64-bit UUID used to identify types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TypeId(pub u64);

impl fmt::Display for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// A timestamp (milliseconds since epoch)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A node in the graph
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for this node
    pub id: NodeId,
    /// When this node was created
    pub timestamp: Timestamp,
    /// Type of this node
    pub type_id: TypeId,
    /// Variable-sized data associated with this node
    pub data: Vec<u8>,
}

/// An arc in the graph connecting two nodes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Arc {
    /// Unique identifier for this arc
    pub id: ArcId,
    /// When this arc was created
    pub timestamp: Timestamp,
    /// Type of this arc
    pub type_id: TypeId,
    /// Source node
    pub from_node: NodeId,
    /// Target node
    pub to_node: NodeId,
    /// Variable-sized data associated with this arc
    pub data: Vec<u8>,
}

impl Node {
    /// Create a new node
    pub fn new(id: NodeId, type_id: TypeId, data: Vec<u8>) -> Self {
        Self {
            id,
            timestamp: Timestamp(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            ),
            type_id,
            data,
        }
    }
}

impl Arc {
    /// Create a new arc
    pub fn new(
        id: ArcId,
        type_id: TypeId,
        from_node: NodeId,
        to_node: NodeId,
        data: Vec<u8>,
    ) -> Self {
        Self {
            id,
            timestamp: Timestamp(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            ),
            type_id,
            from_node,
            to_node,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node_id = NodeId(1);
        let type_id = TypeId(2);
        let data = vec![1, 2, 3, 4];

        let node = Node::new(node_id, type_id, data.clone());

        assert_eq!(node.id, node_id);
        assert_eq!(node.type_id, type_id);
        assert_eq!(node.data, data);
    }

    #[test]
    fn test_arc_creation() {
        let arc_id = ArcId(1);
        let type_id = TypeId(2);
        let from_node = NodeId(3);
        let to_node = NodeId(4);
        let data = vec![1, 2, 3, 4];

        let arc = Arc::new(arc_id, type_id, from_node, to_node, data.clone());

        assert_eq!(arc.id, arc_id);
        assert_eq!(arc.type_id, type_id);
        assert_eq!(arc.from_node, from_node);
        assert_eq!(arc.to_node, to_node);
        assert_eq!(arc.data, data);
    }
}
