use crate::node::Requests;
use crate::{App, Error, LocalServiceId, NodeId, Request, Response, ServiceId};
use anyhow::Result;
use futures::channel::mpsc;
use futures::future::AbortHandle;
use futures::lock::Mutex;
use futures::SinkExt;
use potatonet_common::{bus_message, Context};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// 节点上下文
pub struct NodeContext<'a> {
    /// 请求或者通知来源
    pub(crate) from: Option<ServiceId>,

    /// 当前节点id
    pub(crate) node_id: NodeId,

    /// 当前服务id
    pub(crate) local_service_id: LocalServiceId,

    /// 本地app
    pub(crate) app: &'a App,

    /// 发送消息
    pub(crate) tx: mpsc::Sender<bus_message::Message>,

    /// 未完成请求表
    pub(crate) requests: Arc<Mutex<Requests>>,

    /// 用于停止node的运行
    pub(crate) abort_handle: AbortHandle,
}

impl<'a> NodeContext<'a> {
    /// 请求或者通知来源服务id
    pub fn from(&self) -> Option<ServiceId> {
        self.from
    }

    /// 当前服务id
    pub fn service_id(&self) -> ServiceId {
        self.local_service_id.to_global(self.node_id)
    }

    /// 停止节点运行
    pub fn shutdown_node(&self) {
        self.abort_handle.abort();
    }
}

#[async_trait::async_trait]
impl<'a> Context for NodeContext<'a> {
    async fn call<T, R>(&self, service_name: &str, request: Request<T>) -> Result<Response<R>>
    where
        T: Serialize + Send + Sync,
        R: DeserializeOwned + Send + Sync,
    {
        trace!("call. service={} method={}", service_name, request.method);

        if self.app.services_map.contains_key(service_name) {
            // 优先调用本地服务
            if let Some(lid) = self.app.services_map.get(service_name) {
                if let Some((_, init, service)) = self.app.services.get(lid.to_u32() as usize) {
                    if init.load(Ordering::Relaxed) {
                        let resp = service.call(self, request.to_bytes()).await?;
                        let new_resp = Response::from_bytes(resp);
                        return Ok(new_resp);
                    }
                }
            }
        }

        let (seq, rx) = self.requests.lock().await.add();
        let request = request.to_bytes();
        self.tx
            .clone()
            .send(bus_message::Message::Req {
                seq,
                from: Some(self.local_service_id),
                to_service: service_name.to_string(),
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
        match async_std::future::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(Ok(resp))) => Ok(Response::<R>::from_bytes(resp)),
            Ok(Ok(Err(err))) => Err(anyhow!(err)),
            Ok(Err(_)) => {
                let mut requests = self.requests.lock().await;
                requests.remove(seq);
                Err(Error::Internal.into())
            }
            Err(_) => {
                let mut requests = self.requests.lock().await;
                requests.remove(seq);
                Err(Error::Timeout.into())
            }
        }
    }

    async fn notify<T: Serialize + Send + Sync>(&self, service_name: &str, request: Request<T>) {
        trace!("notify. service={} method={}", service_name, request.method);

        let request = request.to_bytes();

        // 通知本地服务
        if let Some(lid) = self.app.services_map.get(service_name) {
            if let Some((_, init, service)) = self.app.services.get(lid.to_u32() as usize) {
                if init.load(Ordering::Relaxed) {
                    service.notify(self, request.clone()).await;
                }
            }
        }

        // 通知远程服务
        self.tx
            .clone()
            .send(bus_message::Message::SendNotify {
                from: Some(self.local_service_id),
                to_service: service_name.to_string(),
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }
}
