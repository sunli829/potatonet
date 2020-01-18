use crate::{App, NodeContext};
use anyhow::Result;
use async_std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use async_std::stream;
use async_std::task;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::prelude::*;
use futures::select;
use potatonet_common::{bus_message, LocalServiceId, NodeId, Request, Response, ResponseBytes};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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

#[derive(Clone)]
pub struct NodeInner {
    pub app: Arc<App>,
    pub node_id: NodeId,
    pub requests: Arc<Mutex<Requests>>,
    pub tx_abort: mpsc::Sender<()>,
    pub tx_send_msg: mpsc::Sender<bus_message::Message>,
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

    async fn init_services(inner: &NodeInner) {
        // 开始所有服务
        for (idx, (service_name, service)) in inner.app.services.iter().enumerate() {
            info!("start service. name={}", service_name);
            let lid = LocalServiceId(idx as u32);
            service
                .start(&NodeContext {
                    inner: &inner,
                    from: None,
                    service_name: &service_name,
                    local_service_id: LocalServiceId(idx as u32),
                })
                .await;

            // 注册服务
            inner
                .tx_send_msg
                .clone()
                .send(bus_message::Message::RegisterService {
                    name: service_name.clone(),
                    id: lid,
                })
                .await
                .ok();
        }
    }

    async fn process_incoming_msg(inner: &NodeInner, msg: bus_message::Message) {
        match msg {
            // 请求
            bus_message::Message::XReq {
                from,
                to,
                seq,
                method,
                data,
            } => {
                task::spawn({
                    let mut inner = inner.clone();
                    async move {
                        if let Some((service_name, service)) = inner.app.services.get(to.0 as usize)
                        {
                            let res = service
                                .call(
                                    &NodeContext {
                                        inner: &inner,
                                        from: Some(from),
                                        service_name: &service_name,
                                        local_service_id: to,
                                    },
                                    Request::new(method, data),
                                )
                                .await;
                            inner
                                .tx_send_msg
                                .send(bus_message::Message::Rep {
                                    seq,
                                    result: res
                                        .map(|resp| resp.data)
                                        .map_err(|err| err.to_string()),
                                })
                                .await
                                .ok();
                        } else {
                            inner
                                .tx_send_msg
                                .send(bus_message::Message::Rep {
                                    seq,
                                    result: Err("service not found".to_string()),
                                })
                                .await
                                .ok();
                        }
                    }
                });
            }

            // 响应
            bus_message::Message::Rep { seq, result } => {
                if let Some(tx) = inner.requests.lock().await.pending.remove(&seq) {
                    tx.send(result.map(|data| Response::new(data))).ok();
                }
            }

            // 通知
            bus_message::Message::Notify {
                from,
                to_service,
                method,
                data,
            } => {
                task::spawn({
                    let inner = inner.clone();
                    async move {
                        if let Some(lid) = inner.app.services_map.get(&to_service) {
                            if let Some((service_name, service)) =
                                inner.app.services.get(lid.0 as usize)
                            {
                                service
                                    .notify(
                                        &NodeContext {
                                            inner: &inner,
                                            from: Some(from),
                                            service_name: &service_name,
                                            local_service_id: *lid,
                                        },
                                        Request::new(method, data),
                                    )
                                    .await;
                            }
                        }
                    }
                });
            }

            // 系统事件
            bus_message::Message::Event { event } => {
                task::spawn({
                    let inner = inner.clone();
                    async move {
                        for (idx, (service_name, service)) in inner.app.services.iter().enumerate()
                        {
                            service
                                .event(
                                    &NodeContext {
                                        inner: &inner,
                                        from: None,
                                        service_name: &service_name,
                                        local_service_id: LocalServiceId(idx as u32),
                                    },
                                    &event,
                                )
                                .await;
                        }
                    }
                });
            }
            _ => {}
        }
    }

    /// 运行节点
    pub async fn run(self) -> Result<()> {
        let app = Arc::new(self.app);

        // 连接到消息总线
        let stream = TcpStream::connect(
            self.bus_addr
                .unwrap_or_else(|| "127.0.0.1:39901".parse().unwrap()),
        )
        .await?;
        let stream = Arc::new(stream);
        let (tx_incoming_msg, mut rx_incoming_msg) = mpsc::channel(16);
        let (mut tx_outgoing_msg, rx_outgoing_msg) = mpsc::channel(16);

        let (reader_task, abort_reader) =
            future::abortable(bus_message::read_messages(stream.clone(), tx_incoming_msg));
        let reader_handle = task::spawn(reader_task);

        let (writer_task, abort_writer) =
            future::abortable(bus_message::write_messages(stream.clone(), rx_outgoing_msg));
        let writer_handle = task::spawn(writer_task);

        // 心跳发送定时器
        let mut hb_timer = stream::interval(Duration::from_secs(1)).fuse();
        let (tx_abort, mut rx_abort) = mpsc::channel::<()>(1);
        let mut inner = None;

        loop {
            select! {
                _ = rx_abort.next() => {
                    // 退出
                    break;
                }
                _ = hb_timer.next() => {
                    // 发送心跳
                    if let Err(_) = tx_outgoing_msg.send(bus_message::Message::Ping).await {
                        // 连接已断开
                        break;
                    }
                }
                msg = rx_incoming_msg.next() => {
                    if let Some(msg) = msg {
                        if let bus_message::Message::Hello(node_id) = msg {
                            // 节点注册成功
                            inner = Some({
                                let inner = NodeInner {
                                            app: app.clone(),
                                            node_id,
                                            requests: Arc::new(Default::default()),
                                            tx_abort: tx_abort.clone(),
                                            tx_send_msg: tx_outgoing_msg.clone(),
                                        };
                                Self::init_services(&inner).await;
                                inner
                            });
                        } else if let Some(inner) = &inner {
                            Self::process_incoming_msg(&inner, msg).await;
                        }
                    } else {
                        // 连接已断开
                        break;
                    }
                }
            }
        }

        tx_outgoing_msg.send(bus_message::Message::Bye).await.ok();

        abort_reader.abort();
        abort_writer.abort();
        reader_handle.await.ok();
        writer_handle.await.ok();
        Ok(())
    }
}
