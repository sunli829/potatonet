use crate::{Error, Event, NodeContext, Request, Response, Result};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// 服务
#[async_trait::async_trait]
pub trait Service: Sync + Send {
    /// 请求类型
    type Req: Serialize + DeserializeOwned + Send + Sync;

    /// 响应类型
    type Rep: Serialize + DeserializeOwned + Send + Sync;

    /// 通知类型
    type Notify: Serialize + DeserializeOwned + Send + Sync;

    /// 开始服务
    #[allow(unused_variables)]
    async fn start(&self, ctx: &NodeContext<'_>) {}

    /// 停止服务
    #[allow(unused_variables)]
    async fn stop(&self, ctx: &NodeContext<'_>) {}

    /// 功能调用
    #[allow(unused_variables)]
    async fn call(
        &self,
        ctx: &NodeContext<'_>,
        request: Request<Self::Req>,
    ) -> Result<Response<Self::Rep>> {
        bail!(Error::MethodNotFound {
            method: request.method.clone()
        })
    }

    /// 通知
    #[allow(unused_variables)]
    async fn notify(&self, ctx: &NodeContext<'_>, request: Request<Self::Notify>) {}

    /// 系统事件
    #[allow(unused_variables)]
    async fn event(&self, ctx: &NodeContext<'_>, event: &Event) {}
}

/// 命名服务
pub trait NamedService: Service {
    fn name(&self) -> &'static str;
}

/// 服务适配器
pub struct ServiceAdapter<S: Service>(pub S);

pub type DynService = dyn Service<Req = Bytes, Rep = Bytes, Notify = Bytes>;

#[async_trait::async_trait]
impl<S> Service for ServiceAdapter<S>
where
    S: Service,
{
    type Req = Bytes;
    type Rep = Bytes;
    type Notify = Bytes;

    async fn start(&self, ctx: &NodeContext<'_>) {
        self.0.start(ctx).await
    }

    async fn stop(&self, ctx: &NodeContext<'_>) {
        self.0.stop(ctx).await
    }

    async fn call(
        &self,
        ctx: &NodeContext<'_>,
        request: Request<Self::Req>,
    ) -> Result<Response<Self::Rep>> {
        let req = Request::from_bytes(request);
        let resp = self.0.call(ctx, req).await?.to_bytes();
        Ok(resp)
    }

    async fn notify(&self, ctx: &NodeContext<'_>, request: Request<Self::Req>) {
        self.0.notify(ctx, Request::from_bytes(request)).await;
    }

    async fn event(&self, ctx: &NodeContext<'_>, event: &Event) {
        self.0.event(ctx, event).await;
    }
}
