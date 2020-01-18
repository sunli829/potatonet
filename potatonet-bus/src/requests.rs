use potatonet_common::NodeId;
use slab::Slab;

/// 请求管理器
#[derive(Default)]
pub struct Requests {
    /// 未完成的请求
    /// 如果5秒内未收到节点发来的响应，则从该表删除
    pending_requests: Slab<(u32, NodeId)>,
}

impl Requests {
    pub fn add(&mut self, seq: u32, node_id: NodeId) -> u32 {
        self.pending_requests.insert((seq, node_id)) as u32
    }

    pub fn remove(&mut self, seq: u32) -> Option<(u32, NodeId)> {
        if self.pending_requests.contains(seq as usize) {
            Some(self.pending_requests.remove(seq as usize))
        } else {
            None
        }
    }
}
