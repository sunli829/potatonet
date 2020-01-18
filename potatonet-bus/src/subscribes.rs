use potatonet_common::NodeId;
use std::collections::{HashMap, HashSet};

/// 订阅管理器
#[derive(Default)]
pub struct Subscribes {
    subscribes: HashMap<String, HashSet<NodeId>>,
}

impl Subscribes {
    pub fn subscribe(&mut self, topic: String, node_id: NodeId) {
        self.subscribes
            .entry(topic)
            .and_modify(|items| {
                items.insert(node_id);
            })
            .or_insert_with(|| {
                let mut nodes = HashSet::new();
                nodes.insert(node_id);
                nodes
            });
    }

    pub fn unsubscribe(&mut self, topic: String, node_id: NodeId) {
        if let Some(nodes) = self.subscribes.get_mut(&topic) {
            nodes.remove(&node_id);
            if nodes.is_empty() {
                self.subscribes.remove(&topic);
            }
        }
    }

    pub fn remove_node(&mut self, node_id: NodeId) {
        for (_, nodes) in &mut self.subscribes {
            nodes.remove(&node_id);
        }
        self.subscribes.retain(|_, nodes| !nodes.is_empty());
    }

    pub fn query<'a>(&'a self, topic: &str) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        match self.subscribes.get(topic) {
            Some(nodes) => Box::new(nodes.iter().copied()),
            None => Box::new(std::iter::empty()),
        }
    }
}
