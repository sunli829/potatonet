use crate::{App, NodeContext};
use anyhow::Result;
use async_std::net::{SocketAddr, ToSocketAddrs};
use async_std::task;
use futures::channel::mpsc;
use futures::future::Either;
use futures::prelude::*;
use potatonet_client::Client;
use potatonet_common::{bus_message, LocalServiceId, Request};
use std::sync::Arc;

/// 节点构建器
pub struct NodeBuilder {
    bus_addr: Option<SocketAddr>,
    app: App,
}

impl NodeBuilder {
    /// 创建节点
    pub fn new(app: App) -> Self {
        Self {
            bus_addr: None,
            app,
        }
    }

    /// 消息总线地址
    pub async fn bus_addr<A: ToSocketAddrs>(mut self, addr: A) -> Result<Self> {
        self.bus_addr = addr.to_socket_addrs().await?.next();
        Ok(self)
    }

    /// 运行节点
    pub async fn run(self) -> Result<()> {
        let app = Arc::new(self.app);
        let (tx_abort, rx_abort) = mpsc::channel(1);
        let (tx, rx) = mpsc::channel(16);
        let bus_addr = self
            .bus_addr
            .unwrap_or_else(|| "127.0.0.1:39901".parse().unwrap());
        let client = match Client::connect_with_notify(bus_addr, move |msg| {
            let mut tx = tx.clone();
            async move {
                tx.send(msg).await.ok();
            }
        })
        .await
        {
            Ok(client) => Arc::new(client),
            Err(err) => {
                error!("failed to connect to bus. err={}", err);
                return Ok(());
            }
        };

        // 开始所有服务
        for (idx, (service_name, service)) in app.services.iter().enumerate() {
            info!("start service. name={}", service_name);
            let lid = LocalServiceId(idx as u32);
            service
                .start(&NodeContext {
                    client: client.clone(),
                    app: app.clone(),
                    from: None,
                    service_name: &service_name,
                    local_service_id: LocalServiceId(idx as u32),
                    tx_abort: tx_abort.clone(),
                })
                .await;

            // 注册服务
            trace!("register service. name={} lid={}", service_name, lid);
            client.register_service(service_name, lid).await;
        }

        let mut s = stream::select(
            rx.map(|msg| Either::Left(msg)),
            rx_abort.map(|_| Either::Right(())),
        );
        while let Some(msg) = s.next().await {
            match msg {
                // 请求
                Either::Left(bus_message::Message::XReq {
                    from,
                    to,
                    seq,
                    method,
                    data,
                }) => {
                    trace!(
                        "MSG:XREQ seq={} from={} to={} method={}",
                        seq,
                        from,
                        to,
                        method
                    );
                    task::spawn({
                        let app = app.clone();
                        let client = client.clone();
                        let tx_abort = tx_abort.clone();
                        async move {
                            if let Some((service_name, service)) = app.services.get(to.0 as usize) {
                                let res = service
                                    .call(
                                        &NodeContext {
                                            client: client.clone(),
                                            app: app.clone(),
                                            from: Some(from),
                                            service_name: &service_name,
                                            local_service_id: to,
                                            tx_abort,
                                        },
                                        Request::new(method, data),
                                    )
                                    .await;
                                client
                                    .send_msg(bus_message::Message::Rep {
                                        seq,
                                        result: res
                                            .map(|resp| resp.data)
                                            .map_err(|err| err.to_string()),
                                    })
                                    .await;
                            } else {
                                client
                                    .send_msg(bus_message::Message::Rep {
                                        seq,
                                        result: Err("service not found".to_string()),
                                    })
                                    .await;
                            }
                        }
                    });
                }

                // 通知
                Either::Left(bus_message::Message::XNotify {
                    from,
                    to_service,
                    method,
                    data,
                }) => {
                    trace!(
                        "MSG:XNOTIFY from={} to={} method={}",
                        from,
                        to_service,
                        method
                    );

                    task::spawn({
                        let app = app.clone();
                        let client = client.clone();
                        let tx_abort = tx_abort.clone();
                        async move {
                            if let Some(lid) = app.services_map.get(&to_service) {
                                if let Some((service_name, service)) =
                                    app.services.get(lid.0 as usize)
                                {
                                    service
                                        .notify(
                                            &NodeContext {
                                                from: Some(from),
                                                client,
                                                app: app.clone(),
                                                service_name: &service_name,
                                                local_service_id: *lid,
                                                tx_abort: tx_abort.clone(),
                                            },
                                            Request::new(method, data),
                                        )
                                        .await;
                                }
                            }
                        }
                    });
                }

                Either::Right(_) => {
                    break;
                }

                _ => {}
            }
        }

        info!("node shutdown.");
        Ok(())
    }
}
