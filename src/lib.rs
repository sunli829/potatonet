//! PotatoNet微服务框架

#[doc(hidden)]
pub use async_trait;
#[doc(hidden)]
pub use serde_derive;

pub use potatonet_common::*;

#[cfg(feature = "bus")]
/// 消息总线
pub mod bus {
    pub use potatonet_bus::*;
}

#[cfg(feature = "client")]
/// 客户端
pub mod client {
    pub use potatonet_client::*;
}

#[cfg(feature = "codegen")]
pub use potatonet_codegen::{message, service, topic};

#[cfg(feature = "node")]
/// 节点
pub mod node {
    pub use potatonet_node::*;
}
