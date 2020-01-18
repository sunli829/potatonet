#[macro_use]
extern crate log;

use anyhow::Result;
use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};
use async_std::stream;
use async_std::task;
use futures::channel::mpsc::{channel, Sender};
use futures::lock::Mutex;
use futures::prelude::*;
use futures::select;
use potatonet_common::{bus_message, Event, NodeId, ServiceId};
use slab::Slab;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 节点
struct Node {
    /// 节点提供的服务
    services: HashSet<String>,

    /// 上次心跳时间
    hb: Instant,

    /// 发送数据通道
    tx: Sender<bus_message::Message>,
}

#[derive(Default)]
struct Bus {
    nodes: Slab<Node>,
    services: HashMap<String, Vec<ServiceId>>,
    pending_requests: Slab<(u32, NodeId)>,
}

impl Bus {
    fn find_service(&self, name: &str) -> Option<ServiceId> {
        match self.services.get(name) {
            Some(nodes) if !nodes.is_empty() => {
                nodes.get(rand::random::<usize>() % nodes.len()).copied()
            }
            _ => None,
        }
    }

    fn create_node(&mut self, tx: Sender<bus_message::Message>) -> NodeId {
        let id = self.nodes.insert(Node {
            services: Default::default(),
            hb: Instant::now(),
            tx,
        });
        NodeId(id as u32)
    }

    fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(id.0 as usize)
    }

    fn node_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id.0 as usize)
    }
}

pub async fn run<A: ToSocketAddrs>(addr: A) -> Result<()> {
    let bus: Arc<Mutex<Bus>> = Default::default();
    let listener = TcpListener::bind(addr).await?;

    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        if let Ok(stream) = stream {
            task::spawn(client_process(bus.clone(), stream));
        }
    }

    Ok(())
}

async fn process_incoming_msg(bus: Arc<Mutex<Bus>>, node_id: NodeId, msg: bus_message::Message) {
    match msg {
        // 退出
        bus_message::Message::Bye => {
            trace!("[{}/MSG:BYE]", node_id);
        }

        // ping消息
        bus_message::Message::Ping => {
            trace!("[{}/MSG:PING]", node_id);
            bus.lock().await.node_mut(node_id).unwrap().hb = Instant::now();
        }

        // 注册服务
        bus_message::Message::RegisterService { name, id } => {
            trace!("[{}/MSG:REGISTER_SERVICE] name={} id={}", node_id, name, id);
            let mut bus = bus.lock().await;
            let node = bus.node_mut(node_id).unwrap();
            let service_id = id.to_global(node_id);
            node.services.insert(name.clone());
            bus.services
                .entry(name)
                .and_modify(|ids| ids.push(service_id))
                .or_insert_with(|| vec![service_id]);
        }

        // 注销服务
        bus_message::Message::UnregisterService { id } => {
            trace!("[{}/MSG:UNREGISTER_SERVICE] id={}", node_id, id);
            let mut bus = bus.lock().await;
            let service_id = id.to_global(node_id);
            let mut remove_name = None;
            for (name, ids) in &mut bus.services {
                if let Some(pos) = ids.iter().position(|x| *x == service_id) {
                    ids.remove(pos);
                    remove_name = Some(name.clone());
                    break;
                }
            }
            if let Some(name) = remove_name {
                bus.node_mut(node_id).unwrap().services.remove(&name);
            }
        }

        // 请求
        bus_message::Message::Req {
            seq,
            from,
            to_service,
            method,
            data,
        } => {
            trace!(
                "[{}/MSG:REQUEST] seq={} from={} to_service={}, method={}",
                node_id,
                seq,
                from,
                to_service,
                method
            );
            let from = from.to_global(node_id);
            let mut bus = bus.lock().await;
            let to = match bus.find_service(&to_service) {
                Some(to) => to,
                None => {
                    // 服务不存在
                    let err_msg = format!("service '{}' not exists", to_service);
                    bus.node(node_id)
                        .unwrap()
                        .tx
                        .clone()
                        .send(bus_message::Message::Rep {
                            seq,
                            result: Err(err_msg),
                        })
                        .await
                        .ok();
                    return;
                }
            };
            let new_seq = bus.pending_requests.insert((seq, node_id));
            let to_node = bus.node(to.node_id).unwrap();
            to_node
                .tx
                .clone()
                .send(bus_message::Message::XReq {
                    from,
                    to: to.local_service_id,
                    seq: new_seq as u32,
                    method,
                    data,
                })
                .await
                .ok();
        }

        // 响应
        bus_message::Message::Rep { seq, result } => {
            trace!("[{}/MSG:RESPONSE] seq={}", node_id, seq);
            let mut bus = bus.lock().await;
            if let Some((origin_seq, to_node_id)) = bus.pending_requests.get(seq as usize).copied()
            {
                bus.pending_requests.remove(seq as usize);
                if let Some(node) = bus.node(to_node_id) {
                    node.tx
                        .clone()
                        .send(bus_message::Message::Rep {
                            seq: origin_seq,
                            result,
                        })
                        .await
                        .ok();
                }
            };
        }

        // 发送通知
        bus_message::Message::SendNotify {
            from,
            to_service,
            method,
            data,
        } => {
            trace!(
                "[{}/MSG:SEND_NOTIFY] from={} to_service={} method={}",
                node_id,
                from,
                to_service,
                method
            );

            // 通知其它节点的指定服务
            let bus = bus.lock().await;

            if let Some(services) = bus.services.get(&to_service) {
                for service_id in services {
                    if node_id == service_id.node_id {
                        // 不通知来源节点
                        continue;
                    }

                    let to_node = bus.node(service_id.node_id).unwrap();
                    to_node
                        .tx
                        .clone()
                        .send(bus_message::Message::Notify {
                            from: from.to_global(node_id),
                            to_service: to_service.clone(),
                            method,
                            data: data.clone(),
                        })
                        .await
                        .ok();
                }
            }
        }

        // 给指定服务发送通知
        bus_message::Message::SendNotifyTo {
            from,
            to,
            method,
            data,
        } => {
            trace!(
                "[{}/MSG:SEND_NOTIFY_TO] from={} to={} method={}",
                node_id,
                from,
                to,
                method
            );

            // 通知其它节点的指定服务
            let bus = bus.lock().await;
            if let Some(node) = bus.node(to.node_id) {
                node.tx
                    .clone()
                    .send(bus_message::Message::NotifyTo {
                        from: from.to_global(node_id),
                        to: to.local_service_id,
                        method: method,
                        data: data.clone(),
                    })
                    .await
                    .ok();
            }
        }
        _ => {}
    }
}

/// 客户端消息处理
async fn client_process(bus: Arc<Mutex<Bus>>, stream: TcpStream) {
    let stream = Arc::new(stream);
    let (tx_incoming_msg, mut rx_incoming_msg) = channel(16);
    let (mut tx_outgoing_msg, rx_outgoing_msg) = channel(16);
    let node_id = bus.lock().await.create_node(tx_outgoing_msg.clone());

    // 接收消息任务
    // 当心跳超时后，通过abort_handle来关闭消息接收任务
    let (reader_task, abort_reader) =
        future::abortable(bus_message::read_messages(stream.clone(), tx_incoming_msg));
    let reader_handle = task::spawn(reader_task);

    // 写消息任务
    let (writer_task, abort_writer) =
        future::abortable(bus_message::write_messages(stream.clone(), rx_outgoing_msg));
    let writer_handle = task::spawn(writer_task);
    trace!("[{}/CONNECTED]", node_id);

    // 广播节点上线事件
    for (_, node) in &bus.lock().await.nodes {
        node.tx
            .clone()
            .send(bus_message::Message::Event {
                event: Event::NodeUp { id: node_id },
            })
            .await
            .ok();
    }

    // 发送欢迎消息
    tx_outgoing_msg
        .send(bus_message::Message::Hello(node_id))
        .await
        .ok();
    drop(tx_outgoing_msg);

    // 心跳检测定时器
    let mut check_hb = stream::interval(Duration::from_secs(1)).fuse();

    loop {
        select! {
            _ = check_hb.next() => {
                if let Some(node) = bus.lock().await.node(node_id) {
                    if node.hb.elapsed() > Duration::from_secs(30) {
                        // 心跳超时
                        trace!("[{}/MSG:HEARTBEAT_TIMEOUT]", node_id);
                        break;
                    }
                }
            }
            msg = rx_incoming_msg.next() => {
                if let Some(msg) = msg {
                    let mut exit = false;
                    if let bus_message::Message::Bye = &msg {
                        exit = true;
                    }
                    process_incoming_msg(bus.clone(), node_id, msg).await;
                    if exit {
                        // 客户端退出
                    }
                } else {
                    // 连接已断开
                    break;
                }
            }
        }
    }

    // 节点下线
    let mut bus = bus.lock().await;
    for (_, ids) in &mut bus.services {
        ids.retain(|id| id.node_id != node_id);
    }
    bus.nodes.remove(node_id.0 as usize);

    // 广播节点下线事件
    for (_, node) in &bus.nodes {
        node.tx
            .clone()
            .send(bus_message::Message::Event {
                event: Event::NodeDown { id: node_id },
            })
            .await
            .ok();
    }

    // 等待读写任务关闭
    abort_reader.abort();
    abort_writer.abort();
    reader_handle.await.ok();
    writer_handle.await.ok();

    trace!("[{}/DISCONNECTED]", node_id);
}
