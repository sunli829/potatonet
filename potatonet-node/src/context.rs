use crate::App;
use anyhow::Result;
use async_std::task;
use futures::channel::mpsc;
use futures::{Future, FutureExt};
use potatonet_client::{Client, SubscribeId};
use potatonet_common::{Context, LocalServiceId, Request, Response, ServiceId, Topic};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// 节点上下文
pub struct NodeContext<'a> {
    pub(crate) client: Arc<Client>,

    pub(crate) app: Arc<App>,

    /// 请求或者通知来源
    pub(crate) from: Option<ServiceId>,

    // 当前服务名
    pub(crate) service_name: &'a str,

    /// 当前服务id
    pub(crate) local_service_id: LocalServiceId,

    /// 停止节点用通道
    pub(crate) tx_abort: mpsc::Sender<()>,
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
        self.local_service_id.to_global(self.client.node_id())
    }

    /// 停止节点运行
    pub fn shutdown_node(&self) {
        self.tx_abort.clone().try_send(()).ok();
    }
}

impl<'a> NodeContext<'a> {
    /// 订阅指定主题的消息
    pub async fn subscribe_with_topic<T, F, R>(&self, topic: &str, mut handler: F) -> SubscribeId
    where
        T: Topic,
        F: FnMut(&NodeContext<'_>, T) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        // 避免client->context->client的循环引用
        let client = Arc::downgrade(&self.client);
        let app = Arc::downgrade(&self.app);
        let service_name = self.service_name.to_string();
        let local_service_id = self.local_service_id;
        let tx_abort = self.tx_abort.clone();
        self.client
            .subscribe_with_topic(topic, move |msg| {
                let client = client.clone();
                let app = app.clone();
                let service_name = service_name.clone();
                let tx_abort = tx_abort.clone();

                if let (Some(client), Some(app)) = (client.upgrade(), app.upgrade()) {
                    let ctx = NodeContext {
                        client: client.clone(),
                        app: app.clone(),
                        from: None,
                        service_name: &service_name,
                        local_service_id,
                        tx_abort,
                    };
                    handler(&ctx, msg).boxed()
                } else {
                    Box::pin(futures::future::ready(()))
                }
            })
            .await
    }

    /// 订阅消息
    /// 如果重复订阅，则会按订阅顺序依次触发回调函数，并不会增加数据传输流量
    pub async fn subscribe<T, F, R>(&self, handler: F) -> SubscribeId
    where
        T: Topic,
        F: FnMut(&NodeContext<'_>, T) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.subscribe_with_topic(T::name(), handler).await
    }
}

#[async_trait::async_trait]
impl<'a> Context for NodeContext<'a> {
    async fn call<T, R>(&self, service_name: &str, request: Request<T>) -> Result<Response<R>>
    where
        T: Serialize + Send + 'static,
        R: DeserializeOwned + Send + 'static,
    {
        trace!("call. service={} method={}", service_name, request.method);

        if let Some(lid) = self.app.services_map.get(service_name).copied() {
            // 优先调用本地服务
            if let Some((_, service)) = self.app.services.get(lid.0 as usize) {
                let resp = service.call(self, request.to_bytes()).await?;
                return Ok(Response::from_bytes(resp));
            }
        }

        self.client.call(service_name, request).await
    }

    async fn notify<T: Serialize + Send + 'static>(&self, service_name: &str, request: Request<T>) {
        trace!("notify. service={} method={}", service_name, request.method);
        let request = request.to_bytes();

        // 通知本地服务
        if let Some(lid) = self.app.services_map.get(service_name).copied() {
            let client = self.client.clone();
            let app = self.app.clone();
            let service_name = service_name.to_string();
            let from = self.local_service_id.to_global(client.node_id());
            let request = request.clone();
            let tx_abort = self.tx_abort.clone();
            task::spawn(async move {
                if let Some((_, service)) = app.services.get(lid.0 as usize) {
                    service
                        .notify(
                            &NodeContext {
                                client,
                                app: app.clone(),
                                from: Some(from),
                                service_name: &service_name,
                                local_service_id: lid,
                                tx_abort,
                            },
                            request,
                        )
                        .await;
                }
            });
        }

        // 通知远程服务
        self.client.notify(service_name, request).await;
    }

    async fn notify_to<T: Serialize + Send + 'static>(&self, to: ServiceId, request: Request<T>) {
        trace!("notify to. to={} method={}", to, request.method);
        let request = request.to_bytes();

        if to.node_id == self.client.node_id() {
            // 是本地服务
            let client = self.client.clone();
            let app = self.app.clone();
            let from = self.local_service_id.to_global(self.client.node_id());
            let service_name = self.service_name.to_string();
            let tx_abort = self.tx_abort.clone();
            task::spawn(async move {
                if let Some((_, service)) = app.services.get(to.local_service_id.0 as usize) {
                    service.notify(
                        &NodeContext {
                            client,
                            app: app.clone(),
                            from: Some(from),
                            service_name: &service_name,
                            local_service_id: to.local_service_id,
                            tx_abort,
                        },
                        request,
                    );
                }
            });
            return;
        }

        // 通知远程服务
        self.client.notify_to(to, request).await;
    }

    async fn publish_with_topic<T: Topic>(&self, topic: &str, msg: T) {
        trace!("publish. topic={}", topic);
        self.client.publish_with_topic(topic, msg).await;
    }
}
