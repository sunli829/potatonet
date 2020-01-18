#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate anyhow;

#[doc(hidden)]
pub mod bus_message;

mod error;
mod id;
mod request;

pub use error::*;
pub use id::*;
pub use request::*;

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Cursor;

pub type Result<T> = anyhow::Result<T>;

/// 订阅消息
pub trait Topic: Serialize + DeserializeOwned + Send + Sync + 'static {
    fn name() -> &'static str;

    fn encode(&self) -> Result<Vec<u8>> {
        let data = rmp_serde::to_vec(self)?;
        Ok(data)
    }

    fn decode(data: &[u8]) -> Result<Self> {
        let msg = rmp_serde::from_read(Cursor::new(data))?;
        Ok(msg)
    }
}

/// 上下文
#[async_trait::async_trait]
pub trait Context {
    /// 调用服务
    async fn call<T, R>(&self, service_name: &str, request: Request<T>) -> Result<Response<R>>
    where
        T: Serialize + Send + 'static,
        R: DeserializeOwned + Send + 'static;

    /// 发送通知
    async fn notify<T: Serialize + Send + 'static>(&self, service_name: &str, request: Request<T>);

    /// 给指定服务发送通知
    async fn notify_to<T: Serialize + Send + 'static>(&self, to: ServiceId, request: Request<T>);

    /// 发布消息
    async fn publish_with_topic<T: Topic>(&self, topic: &str, msg: T);

    /// 发布消息
    async fn publish<T: Topic>(&self, msg: T) {
        self.publish_with_topic(T::name(), msg).await
    }
}
