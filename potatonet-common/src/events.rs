use crate::NodeId;

/// 系统事件
#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    /// 节点上线事件
    NodeUp {
        /// 节点id
        id: NodeId,
    },

    /// 节点下线事件
    NodeDown {
        /// 节点id
        id: NodeId,
    },
}
