use std::fmt::{Display, Formatter, Result};

/// 节点Id
/// 8位最多256个节点
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct NodeId(u8);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "NID({})", self.0)
    }
}

impl NodeId {
    #[inline]
    pub fn from_u8(id: u8) -> Self {
        Self(id)
    }

    #[inline]
    pub fn to_u8(&self) -> u8 {
        self.0
    }
}

/// 全局服务Id
/// 前面1字节是节点Id，后面3个字节是本地服务Id
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ServiceId(u32);

impl Display for ServiceId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "SID({}:{})", self.node_id().0, self.local_service_id().0)
    }
}

impl ServiceId {
    #[inline]
    pub fn from_u32(id: u32) -> Self {
        Self(id)
    }

    #[inline]
    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

/// 本地24位服务Id
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct LocalServiceId(u32);

impl LocalServiceId {
    #[inline]
    pub fn from_u32(id: u32) -> Self {
        Self(id)
    }

    #[inline]
    pub fn to_u32(&self) -> u32 {
        self.0
    }

    /// 转换为全局服务id
    #[inline]
    pub fn to_global(self, node_id: NodeId) -> ServiceId {
        ServiceId((node_id.0 as u32) << 24 | (self.0) as u32)
    }
}

impl Display for LocalServiceId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "LID({})", self.0)
    }
}

impl ServiceId {
    /// 获取节点Id
    #[inline]
    pub fn node_id(&self) -> NodeId {
        NodeId((self.0 >> 24) as u8)
    }

    /// 获取本地服务id
    #[inline]
    pub fn local_service_id(&self) -> LocalServiceId {
        LocalServiceId(self.0 & 0xffffff)
    }
}
