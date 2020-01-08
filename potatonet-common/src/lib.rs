#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate thiserror;

pub mod bus_message;
mod error;
mod events;
mod id;
mod request;

pub use error::*;
pub use events::*;
pub use id::*;
pub use request::*;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub type Result<T> = anyhow::Result<T>;

/// 上下文
#[async_trait::async_trait]
pub trait Context {
    /// 调用服务
    async fn call<T, R>(&self, service_name: &str, request: Request<T>) -> Result<Response<R>>
    where
        T: Serialize + Send + Sync,
        R: DeserializeOwned + Send + Sync;

    /// 发送通知
    async fn notify<T: Serialize + Send + Sync>(&self, service_name: &str, request: Request<T>);
}
