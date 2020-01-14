use crate::{App, NodeContext, Request, Response, ResponseBytes};
use anyhow::Result;
use async_std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use async_std::task;
use futures::channel::{mpsc, oneshot};
use futures::future::{AbortHandle, Abortable};
use futures::lock::Mutex;
use futures::prelude::*;
use potatonet_common::bus_message::Message;
use potatonet_common::{bus_message, LocalServiceId};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Default)]
pub struct Requests {
    pending: HashMap<u32, oneshot::Sender<std::result::Result<ResponseBytes, String>>>,
    seq: u32,
}

impl Requests {
    pub fn add(
        &mut self,
    ) -> (
        u32,
        oneshot::Receiver<std::result::Result<ResponseBytes, String>>,
    ) {
        let (tx, rx) = oneshot::channel();
        self.seq += 1;
        self.pending.insert(self.seq, tx);
        (self.seq, rx)
    }

    pub fn remove(&mut self, seq: u32) {
        self.pending.remove(&seq);
    }
}

/// 节点构建器
pub struct NodeBuilder {
    bus_addr: Option<SocketAddr>,
    app: App,
    name: Option<String>,
}

impl NodeBuilder {
    pub fn new(app: App) -> Self {
        Self {
            bus_addr: None,
            app,
            name: None,
        }
    }

    /// 消息总线地址
    pub async fn bus_addr<A: ToSocketAddrs>(mut self, addr: A) -> Result<Self> {
        self.bus_addr = addr.to_socket_addrs().await?.next();
        Ok(self)
    }

    /// 节点名称
    pub fn name<N: Into<String>>(mut self, name: N) -> Self {
        self.name = Some(name.into());
        self
    }

    /// 运行节点
    pub async fn run(self) -> Result<()> {
        // 连接到消息总线
        let stream = TcpStream::connect(
            self.bus_addr
                .unwrap_or_else(|| "127.0.0.1:39901".parse().unwrap()),
        )
        .await?;

        // 发送hello消息，并等待服务响应
        let name = self
            .name
            .unwrap_or_else(|| names::Generator::default().next().unwrap());
        bus_message::write_message(&stream, &bus_message::Message::RegisterNode(name.clone()))
            .await?;
        let node_id = match bus_message::read_message(&stream).await {
            Ok(bus_message::Message::NodeRegistered(node_id)) => node_id,
            res => {
                println!("{:?}", res);
                bail!("unable connect to bus")
            }
        };
        info!("bus connected. node_id={}", node_id);

        // 创建接收和发送消息任务
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let (mut tx, mut rx) = mpsc::channel(16);
        let stream = Arc::new(stream);

        // 发送任务
        let send_handle = task::spawn({
            let stream = stream.clone();
            async move {
                while let Some(msg) = rx.next().await {
                    if let Err(_) = bus_message::write_message(&*stream, &msg).await {
                        return;
                    }
                }
            }
        });

        // 处理消息
        let requests = Arc::new(Mutex::new(Requests::default()));
        let app = Arc::new(self.app);

        let recv_handle = task::spawn({
            let app = app.clone();
            let requests = requests.clone();
            let tx = tx.clone();
            let abort_handle = abort_handle.clone();
            Abortable::new(
                async move {
                    while let Ok(msg) = bus_message::read_message(&*stream).await {
                        match msg {
                            bus_message::Message::XReq {
                                from,
                                to,
                                seq,
                                method,
                                data,
                            } => {
                                task::spawn({
                                    let app = app.clone();
                                    let abort_handle = abort_handle.clone();
                                    let mut tx = tx.clone();
                                    let requests = requests.clone();

                                    async move {
                                        if let Some((_, init, service)) =
                                            app.services.get(to.to_u32() as usize)
                                        {
                                            if init.load(Ordering::Relaxed) {
                                                let res = service
                                                    .call(
                                                        &NodeContext {
                                                            from,
                                                            node_id,
                                                            local_service_id: to,
                                                            app: &app,
                                                            tx: tx.clone(),
                                                            requests,
                                                            abort_handle,
                                                        },
                                                        Request::new(method, data),
                                                    )
                                                    .await;
                                                tx.send(bus_message::Message::Rep {
                                                    seq,
                                                    result: res
                                                        .map(|resp| resp.data)
                                                        .map_err(|err| err.to_string()),
                                                })
                                                .await
                                                .ok();
                                            } else {
                                                tx.send(bus_message::Message::Rep {
                                                    seq,
                                                    result: Err(
                                                        "service not initialized".to_string()
                                                    ),
                                                })
                                                .await
                                                .ok();
                                            }
                                        } else {
                                            tx.send(bus_message::Message::Rep {
                                                seq,
                                                result: Err("service not found".to_string()),
                                            })
                                            .await
                                            .ok();
                                        }
                                    }
                                });
                            }
                            bus_message::Message::Rep { seq, result } => {
                                if let Some(tx) = requests.lock().await.pending.remove(&seq) {
                                    tx.send(result.map(|data| Response::new(data))).ok();
                                }
                            }
                            bus_message::Message::Notify {
                                from,
                                to_service,
                                method,
                                data,
                            } => {
                                task::spawn({
                                    let app = app.clone();
                                    let abort_handle = abort_handle.clone();
                                    let tx = tx.clone();
                                    let requests = requests.clone();

                                    async move {
                                        if let Some(lid) = app.services_map.get(&to_service) {
                                            if let Some((_, init, service)) =
                                                app.services.get(lid.to_u32() as usize)
                                            {
                                                if init.load(Ordering::Relaxed) {
                                                    service
                                                        .notify(
                                                            &NodeContext {
                                                                from,
                                                                node_id,
                                                                local_service_id: *lid,
                                                                app: &app,
                                                                tx: tx.clone(),
                                                                requests,
                                                                abort_handle,
                                                            },
                                                            Request::new(method, data),
                                                        )
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                            bus_message::Message::Event { event } => {
                                task::spawn({
                                    let app = app.clone();
                                    let abort_handle = abort_handle.clone();
                                    let tx = tx.clone();
                                    let requests = requests.clone();

                                    async move {
                                        for (idx, (_, init, service)) in
                                            app.services.iter().enumerate()
                                        {
                                            if init.load(Ordering::Relaxed) {
                                                service
                                                        .event(
                                                            &NodeContext {
                                                                from: None,
                                                                node_id,
                                                                local_service_id: LocalServiceId::from_u32(
                                                                    idx as u32,
                                                                ),
                                                                app: &app,
                                                                tx: tx.clone(),
                                                                requests: requests.clone(),
                                                                abort_handle: abort_handle.clone(),
                                                            },
                                                            &event,
                                                        )
                                                        .await;
                                            }
                                        }
                                    }
                                });
                            }
                            _ => {}
                        }
                    }
                },
                abort_registration,
            )
        });

        // 初始化所有服务
        for (idx, (name, init, service)) in app.services.iter().enumerate() {
            info!("initialize service. name={}", name);
            let lid = LocalServiceId::from_u32(idx as u32);
            service
                .init(&NodeContext {
                    from: None,
                    node_id,
                    local_service_id: lid,
                    app: &app,
                    tx: tx.clone(),
                    requests: requests.clone(),
                    abort_handle: abort_handle.clone(),
                })
                .await;
            init.store(true, Ordering::Relaxed);

            // 注册服务
            tx.send(bus_message::Message::RegisterService {
                name: name.clone(),
                id: lid,
            })
            .await
            .ok();
        }

        recv_handle.await.ok();
        tx.send(Message::UnregisterNode).await.ok();
        drop(tx);
        send_handle.await;
        Ok(())
    }
}
