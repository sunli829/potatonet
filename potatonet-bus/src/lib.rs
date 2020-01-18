#![recursion_limit = "512"]

mod requests;
mod subscribes;

#[macro_use]
extern crate log;

use crate::requests::Requests;
use crate::subscribes::Subscribes;
use anyhow::Result;
use async_std::net::{TcpListener, TcpStream, ToSocketAddrs};
use async_std::stream;
use async_std::task;
use futures::channel::mpsc::{channel, Sender};
use futures::lock::Mutex;
use futures::prelude::*;
use futures::select;
use potatonet_common::{bus_message, LocalServiceId, NodeId, ServiceId};
use slab::Slab;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 节点
struct Node {
    /// 节点提供的服务
    services: HashMap<LocalServiceId, String>,

    /// 上次心跳时间
    hb: Instant,

    /// 主动断开节点连接通知通道
    tx_close: Sender<()>,

    /// 发送数据通道
    tx: Sender<bus_message::Message>,
}

/// 消息总线数据
#[derive(Default)]
struct Bus {
    /// 节点集合
    nodes: Slab<Node>,

    /// 按服务名索引的服务id
    services: HashMap<String, Vec<ServiceId>>,

    /// 未完成的请求
    /// 如果5秒内未收到节点发来的响应，则从该表删除
    pending_requests: Requests,

    /// 订阅信息
    subscribes: Subscribes,
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

    fn create_node(&mut self, tx: Sender<bus_message::Message>, tx_close: Sender<()>) -> NodeId {
        let id = self.nodes.insert(Node {
            services: Default::default(),
            hb: Instant::now(),
            tx_close,
            tx,
        });
        NodeId(id as u32)
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
            if let Some(node) = bus.lock().await.nodes.get_mut(node_id.0 as usize) {
                node.hb = Instant::now();
            }
        }

        // 注册服务
        bus_message::Message::RegisterService { name, id } => {
            trace!("[{}/MSG:REGISTER_SERVICE] name={} id={}", node_id, name, id);
            let mut bus = bus.lock().await;
            if let Some(node) = bus.nodes.get_mut(node_id.0 as usize) {
                let service_id = id.to_global(node_id);
                node.services.insert(id, name.clone());
                bus.services
                    .entry(name)
                    .and_modify(|ids| ids.push(service_id))
                    .or_insert_with(|| vec![service_id]);
            }
        }

        // 注销服务
        bus_message::Message::UnregisterService { id } => {
            trace!("[{}/MSG:UNREGISTER_SERVICE] id={}", node_id, id);
            let mut bus = bus.lock().await;
            let service_id = id.to_global(node_id);
            for (_, ids) in &mut bus.services {
                if let Some(pos) = ids.iter().position(|x| *x == service_id) {
                    ids.remove(pos);
                    break;
                }
            }
            if let Some(node) = bus.nodes.get_mut(node_id.0 as usize) {
                node.services.remove(&id);
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
            let mut bus_inner = bus.lock().await;
            let to = match bus_inner.find_service(&to_service) {
                Some(to) => to,
                None => {
                    // 服务不存在
                    let err_msg = format!("service '{}' not exists", to_service);
                    if let Some(node) = bus_inner.nodes.get_mut(node_id.0 as usize) {
                        if let Err(_) = node.tx.try_send(bus_message::Message::Rep {
                            seq,
                            result: Err(err_msg),
                        }) {
                            // 数据发送失败，断开连接
                            node.tx_close.try_send(()).ok();
                        }
                    }
                    return;
                }
            };
            let new_seq = bus_inner.pending_requests.add(seq, node_id);
            if let Some(to_node) = bus_inner.nodes.get_mut(to.node_id.0 as usize) {
                if let Err(_) = to_node.tx.try_send(bus_message::Message::XReq {
                    from,
                    to: to.local_service_id,
                    seq: new_seq as u32,
                    method,
                    data,
                }) {
                    // 数据发送失败，断开连接
                    to_node.tx_close.try_send(()).ok();
                }
            }

            // 5秒未收到响应则删除
            task::spawn({
                let bus = bus.clone();
                async move {
                    task::sleep(Duration::from_secs(5)).await;
                    let mut bus = bus.lock().await;
                    bus.pending_requests.remove(new_seq);
                }
            });
        }

        // 响应
        bus_message::Message::Rep { seq, result } => {
            trace!("[{}/MSG:RESPONSE] seq={}", node_id, seq);
            let mut bus = bus.lock().await;
            if let Some((origin_seq, to_node_id)) = bus.pending_requests.remove(seq) {
                if let Some(node) = bus.nodes.get_mut(to_node_id.0 as usize) {
                    if let Err(_) = node.tx.try_send(bus_message::Message::Rep {
                        seq: origin_seq,
                        result,
                    }) {
                        // 数据发送失败，断开连接
                        node.tx_close.try_send(()).ok();
                    }
                }
            };
        }

        // 发送通知
        bus_message::Message::Notify {
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
            let mut bus = bus.lock().await;
            let bus = &mut *bus;

            if let Some(services) = bus.services.get(&to_service) {
                for service_id in services {
                    if node_id == service_id.node_id {
                        // 不通知来源节点
                        continue;
                    }

                    let to_node = bus.nodes.get_mut(service_id.node_id.0 as usize).unwrap();
                    if let Err(_) = to_node.tx.try_send(bus_message::Message::XNotify {
                        from: from.to_global(node_id),
                        to_service: to_service.clone(),
                        method,
                        data: data.clone(),
                    }) {
                        // 数据发送失败，断开连接
                        to_node.tx_close.try_send(()).ok();
                    }
                }
            }
        }

        // 给指定服务发送通知
        bus_message::Message::NotifyTo {
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
            let mut bus = bus.lock().await;
            if let Some(node) = bus.nodes.get_mut(to.node_id.0 as usize) {
                if let Err(_) = node.tx.try_send(bus_message::Message::XNotifyTo {
                    from: from.to_global(node_id),
                    to: to.local_service_id,
                    method: method,
                    data: data.clone(),
                }) {
                    // 数据发送失败，断开连接
                    node.tx_close.try_send(()).ok();
                }
            }
        }

        // 订阅
        bus_message::Message::Subscribe { topic } => {
            trace!("[{}/MSG:SUBSCRIBE] topic={}", node_id, topic);
            let mut bus = bus.lock().await;
            bus.subscribes.subscribe(topic, node_id);
        }

        // 取消订阅
        bus_message::Message::Unsubscribe { topic } => {
            trace!("[{}/MSG:UNSUBSCRIBE] topic={}", node_id, topic);
            let mut bus = bus.lock().await;
            bus.subscribes.unsubscribe(topic, node_id);
        }

        // 发布消息
        bus_message::Message::Publish { topic, data } => {
            trace!("[{}/MSG:PUBLISH] topic={}", node_id, topic);
            let mut bus = bus.lock().await;
            let bus = &mut *bus;
            for to_node_id in bus.subscribes.query(&topic) {
                if let Some(to_node) = bus.nodes.get_mut(to_node_id.0 as usize) {
                    if let Err(_) = to_node.tx.try_send(bus_message::Message::XPublish {
                        topic: topic.clone(),
                        data: data.clone(),
                    }) {
                        // 数据发送失败，断开连接
                        to_node.tx_close.try_send(()).ok();
                    }
                }
            }
        }

        _ => {}
    }
}

/// 客户端消息处理
async fn client_process(bus: Arc<Mutex<Bus>>, stream: TcpStream) {
    let stream = Arc::new(stream);
    let (tx_close, mut rx_close) = channel(1);
    let (tx_incoming_msg, mut rx_incoming_msg) = channel(64);
    let (mut tx_outgoing_msg, rx_outgoing_msg) = channel(64);
    let node_id = bus
        .lock()
        .await
        .create_node(tx_outgoing_msg.clone(), tx_close);

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

    // 发送欢迎消息
    tx_outgoing_msg
        .try_send(bus_message::Message::Hello(node_id))
        .ok();
    drop(tx_outgoing_msg);

    // 心跳检测定时器
    let mut check_hb = stream::interval(Duration::from_secs(1)).fuse();

    loop {
        select! {
            _ = rx_close.next() => {
                // 主动断开连接
                break;
            }
            _ = check_hb.next() => {
                if let Some(node) = bus.lock().await.nodes.get(node_id.0 as usize) {
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
                        break;
                    }
                } else {
                    // 连接已断开
                    trace!("client connection close. node_id={}", node_id);
                    break;
                }
            }
        }
    }

    // 节点下线
    let mut bus = bus.lock().await;
    bus.subscribes.remove_node(node_id);
    for (_, ids) in &mut bus.services {
        ids.retain(|id| id.node_id != node_id);
    }
    bus.nodes.remove(node_id.0 as usize);

    // 等待读写任务关闭
    abort_reader.abort();
    abort_writer.abort();
    reader_handle.await.ok();
    writer_handle.await.ok();

    trace!("[{}/DISCONNECTED]", node_id);
}
