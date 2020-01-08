use crate::NodeId;

/// 错误
#[derive(Error, Debug)]
pub enum Error {
    #[error("method '{method}' not found")]
    MethodNotFound { method: String },

    #[error("service '{service_name}' not found")]
    ServiceNotFound { service_name: String },

    #[error("node '{id}' not exists")]
    NodeNotExists { id: NodeId },

    #[error("timeout")]
    Timeout,

    #[error("internal")]
    Internal,
}
