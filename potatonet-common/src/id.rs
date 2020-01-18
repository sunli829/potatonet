use std::fmt::{Display, Formatter, Result};

/// 节点Id
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u32);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "NID({})", self.0)
    }
}

/// 全局服务Id
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ServiceId {
    pub node_id: NodeId,
    pub local_service_id: LocalServiceId,
}

impl Display for ServiceId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "SID({}:{})", self.node_id.0, self.local_service_id.0)
    }
}

/// 本地24位服务Id
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct LocalServiceId(pub u32);

impl LocalServiceId {
    /// 转换为全局服务id
    #[inline]
    pub fn to_global(self, node_id: NodeId) -> ServiceId {
        ServiceId {
            node_id,
            local_service_id: self,
        }
    }
}

impl Display for LocalServiceId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "LID({})", self.0)
    }
}
