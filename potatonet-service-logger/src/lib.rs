mod macros;
mod mongo_backend;
mod stdout_backend;

use chrono::{DateTime, Utc};
use potatonet_node::*;
use rmpv::Value;
use std::collections::HashMap;

#[doc(hidden)]
pub use chrono;

pub use mongo_backend::{MongoBackend, MongoBackendConfig};
pub use stdout_backend::StdoutBackend;

#[message]
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

#[message]
pub struct Item {
    pub time: DateTime<Utc>,
    pub service: String,
    pub level: Level,
    pub message: String,
    pub kvs: Option<HashMap<String, Value>>,
}

/// 日志后台
pub trait Backend: Sync + Send + 'static {
    /// 添加日志
    fn append(&self, item: Item) -> Result<()>;

    /// 查询最新日志
    fn query_latest(&self, limit: usize) -> Result<Vec<Item>>;
}

/// 日志服务
pub struct Logger {
    backend: Box<dyn Backend>,
}

impl Logger {
    /// 新建日志服务
    pub fn new<B: Backend>(backend: B) -> Self {
        Self {
            backend: Box::new(backend),
        }
    }
}

#[service]
impl Logger {
    #[notify]
    async fn log(&self, item: Item) {
        self.backend.append(item).ok();
    }

    #[call]
    async fn query_latest(&self, limit: usize) -> Result<Vec<Item>> {
        self.backend.query_latest(limit)
    }
}
