use crate::node::NodeInner;
use anyhow::Result;
use async_std::task;
use futures::SinkExt;
use potatonet_common::{bus_message, Context, Error, LocalServiceId, Request, Response, ServiceId};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

/// 节点上下文
pub struct NodeContext<'a> {
    pub(crate) inner: &'a NodeInner,

    /// 请求或者通知来源
    pub(crate) from: Option<ServiceId>,

    // 当前服务名
    pub(crate) service_name: &'a str,

    /// 当前服务id
    pub(crate) local_service_id: LocalServiceId,
}

impl<'a> NodeContext<'a> {
    /// 请求或者通知来源服务id
    pub fn from(&self) -> Option<ServiceId> {
        self.from
    }

    /// 当前服务名称
    pub fn service_name(&self) -> &str {
        self.service_name
    }

    /// 当前服务id
    pub fn service_id(&self) -> ServiceId {
        self.local_service_id.to_global(self.inner.node_id)
    }

    /// 停止节点运行
    pub fn shutdown_node(&self) {
        self.inner.tx_abort.clone().start_send(()).ok();
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

        if let Some(lid) = self.inner.app.services_map.get(service_name).copied() {
            // 优先调用本地服务
            if let Some((_, service)) = self.inner.app.services.get(lid.0 as usize) {
                let resp = service.call(self, request.to_bytes()).await?;
                return Ok(Response::from_bytes(resp));
            }
        }

        let (seq, rx) = self.inner.requests.lock().await.add();
        let request = request.to_bytes();
        self.inner
            .tx_send_msg
            .clone()
            .send(bus_message::Message::Req {
                seq,
                from: self.local_service_id,
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
                let mut requests = self.inner.requests.lock().await;
                requests.remove(seq);
                Err(Error::Internal.into())
            }
            Err(_) => {
                let mut requests = self.inner.requests.lock().await;
                requests.remove(seq);
                Err(Error::Timeout.into())
            }
        }
    }

    async fn notify<T: Serialize + Send + Sync>(&self, service_name: &str, request: Request<T>) {
        trace!("notify. service={} method={}", service_name, request.method);
        let request = request.to_bytes();

        // 通知本地服务
        if let Some(lid) = self.inner.app.services_map.get(service_name).copied() {
            let inner = self.inner.clone();
            let service_name = service_name.to_string();
            let from = self.local_service_id.to_global(inner.node_id);
            let request = request.clone();
            task::spawn(async move {
                if let Some((_, service)) = inner.app.services.get(lid.0 as usize) {
                    service
                        .notify(
                            &NodeContext {
                                inner: &inner,
                                from: Some(from),
                                service_name: &service_name,
                                local_service_id: lid,
                            },
                            request,
                        )
                        .await;
                }
            });
        }

        // 通知远程服务
        self.inner
            .tx_send_msg
            .clone()
            .send(bus_message::Message::SendNotify {
                from: self.local_service_id,
                to_service: service_name.to_string(),
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }

    async fn notify_to<T: Serialize + Send + Sync>(&self, to: ServiceId, request: Request<T>) {
        trace!("notify to. to={} method={}", to, request.method);
        let request = request.to_bytes();

        if to.node_id == self.inner.node_id {
            // 是本地服务
            let inner = self.inner.clone();
            let from = self.local_service_id.to_global(inner.node_id);
            let service_name = self.service_name.to_string();
            task::spawn(async move {
                if let Some((_, service)) = inner.app.services.get(to.local_service_id.0 as usize) {
                    service.notify(
                        &NodeContext {
                            inner: &inner,
                            from: Some(from),
                            service_name: &service_name,
                            local_service_id: to.local_service_id,
                        },
                        request,
                    );
                }
            });
        } else {
            // 通知远程服务
            self.inner
                .tx_send_msg
                .clone()
                .send(bus_message::Message::SendNotifyTo {
                    from: self.local_service_id,
                    to,
                    method: request.method,
                    data: request.data,
                })
                .await
                .ok();
        }
    }
}
