#[macro_use]
extern crate log;

use anyhow::Result;
use async_std::net::{TcpListener, ToSocketAddrs};
use async_std::task;
use futures::channel::mpsc::{channel, Sender};
use futures::future::select_all;
use futures::lock::Mutex;
use futures::prelude::*;
use potatonet_common::{bus_message, Event, NodeId, ServiceId};
use slab::Slab;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 会话信息
enum Session {
    /// 节点会话
    Node {
        id: NodeId,
        name: String,
        tx: Sender<bus_message::Message>,
        hb: Instant,
    },
    /// 客户端会话
    Client { tx: Sender<bus_message::Message> },
}

#[derive(Default)]
struct Bus {
    sessions: Slab<Session>,
    nodes: Slab<usize>,
    services: HashMap<String, Vec<ServiceId>>,
    services_set: HashSet<ServiceId>,
    pending_requests: Slab<(u32, usize)>,
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
}

pub async fn run<A: ToSocketAddrs>(addr: A) -> Result<()> {
    let bus: Arc<Mutex<Bus>> = Default::default();
    let listener = TcpListener::bind(addr).await?;

    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = Arc::new(stream?);

        // 创建一个会话，默认为客户端会话类型
        let (tx, mut rx) = channel(16);
        let session_id = {
            let mut bus = bus.lock().await;
            let entry = bus.sessions.vacant_entry();
            let id = entry.key();
            entry.insert(Session::Client { tx: tx.clone() });
            id
        };

        let recv_fut = {
            let stream = stream.clone();
            let bus = bus.clone();
            let mut tx = tx.clone();
            async move {
                while let Ok(msg) = bus_message::read_message(&*stream).await {
                    match msg {
                        // 注销节点
                        bus_message::Message::UnregisterNode => {
                            trace!("[{}/MSG:UNREGISTER_NODE]", session_id);
                            return;
                        }

                        // 节点发送的ping消息
                        bus_message::Message::Ping => {
                            trace!("[{}/MSG:PING]", session_id);
                            let mut bus = bus.lock().await;
                            let session = bus.sessions.get_mut(session_id).unwrap();
                            if let Session::Node { hb, .. } = session {
                                *hb = Instant::now();
                            }
                        }

                        // 注册节点
                        // 把当前客户端会话类型转换为节点会话类型
                        bus_message::Message::RegisterNode(name) => {
                            trace!("[{}/MSG:REGISTER_NODE] name={}", session_id, name);

                            let mut bus = bus.lock().await;
                            let session = bus.sessions.get(session_id).unwrap();
                            if let Session::Client { tx } = session {
                                // 分配节点id
                                let mut tx = tx.clone();
                                let node_id = NodeId::from_u8(bus.nodes.insert(session_id) as u8);
                                std::mem::replace(
                                    bus.sessions.get_mut(session_id).unwrap(),
                                    Session::Node {
                                        id: node_id,
                                        name: name.clone(),
                                        tx: tx.clone(),
                                        hb: Instant::now(),
                                    },
                                );

                                // 发送注册成功消息
                                tx.send(bus_message::Message::NodeRegistered(node_id))
                                    .await
                                    .ok();

                                // 广播节点上线事件
                                for (id, session_id) in &bus.nodes {
                                    let session = bus.sessions.get(*session_id).unwrap();
                                    if id != node_id.to_u8() as usize {
                                        if let Session::Node { tx, .. } = session {
                                            tx.clone()
                                                .send(bus_message::Message::Event {
                                                    event: Event::NodeUp {
                                                        name: name.clone(),
                                                        id: node_id,
                                                    },
                                                })
                                                .await
                                                .ok();
                                        }
                                    }
                                }
                            } else {
                                // 当前已经是节点会话了，不能重复发送
                                return;
                            }
                        }

                        // 注册服务
                        bus_message::Message::RegisterService { name, id } => {
                            trace!(
                                "[{}/MSG:REGISTER_SERVICE] name={} id={}",
                                session_id,
                                name,
                                id
                            );
                            let mut bus = bus.lock().await;
                            let session = bus.sessions.get_mut(session_id).unwrap();
                            if let Session::Node { id: node_id, .. } = session {
                                let service_id = id.to_global(*node_id);
                                bus.services
                                    .entry(name)
                                    .and_modify(|ids| ids.push(service_id))
                                    .or_insert_with(|| vec![service_id]);
                                bus.services_set.insert(service_id);
                            }
                        }

                        // 注销服务
                        bus_message::Message::UnregisterService { id } => {
                            trace!("[{}/MSG:UNREGISTER_SERVICE] id={}", session_id, id);
                            let mut bus = bus.lock().await;
                            let session = bus.sessions.get_mut(session_id).unwrap();
                            if let Session::Node { id: node_id, .. } = session {
                                let service_id = id.to_global(*node_id);
                                bus.services_set.remove(&service_id);
                                for (_, ids) in &mut bus.services {
                                    if let Some(pos) = ids.iter().position(|x| *x == service_id) {
                                        ids.remove(pos);
                                        break;
                                    }
                                }
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
                                session_id,
                                seq,
                                from.map(|from| Cow::Owned(from.to_string()))
                                    .unwrap_or_else(|| Cow::Borrowed("<unknown>")),
                                to_service,
                                method
                            );
                            let (from, to) = {
                                let mut bus = bus.lock().await;
                                let session = bus.sessions.get_mut(session_id).unwrap();
                                let from = match (session, from) {
                                    (Session::Node { id, .. }, Some(lid)) => {
                                        Some(lid.to_global(*id))
                                    }
                                    _ => None,
                                };
                                let to = match bus.find_service(&to_service) {
                                    Some(sid) => {
                                        let session = bus
                                            .sessions
                                            .get(
                                                *bus.nodes
                                                    .get(sid.node_id().to_u8() as usize)
                                                    .unwrap(),
                                            )
                                            .unwrap();
                                        if let Session::Node { tx, .. } = session {
                                            let tx = tx.clone();
                                            let seq =
                                                bus.pending_requests.insert((seq, session_id));
                                            Ok((seq as u32, tx, sid))
                                        } else {
                                            return;
                                        }
                                    }
                                    None => Err(format!("service '{}' not exists", to_service)),
                                };
                                (from, to)
                            };

                            match to {
                                Ok((new_seq, mut tx, to)) => {
                                    tx.send(bus_message::Message::XReq {
                                        from,
                                        to: to.local_service_id(),
                                        seq: new_seq,
                                        method,
                                        data,
                                    })
                                    .await
                                    .ok();
                                }
                                Err(err) => {
                                    tx.send(bus_message::Message::Rep {
                                        seq,
                                        result: Err(err),
                                    })
                                    .await
                                    .ok();
                                }
                            }
                        }

                        // 响应
                        bus_message::Message::Rep { seq, result } => {
                            trace!("[{}/MSG:RESPONSE] seq={}", session_id, seq);
                            let mut bus = bus.lock().await;
                            let pr = if let Some((origin_seq, session_id)) =
                                bus.pending_requests.get(seq as usize).copied()
                            {
                                bus.pending_requests.remove(seq as usize);
                                if let Some(session) = bus.sessions.get(session_id) {
                                    match session {
                                        Session::Client { tx } => Some((tx.clone(), origin_seq)),
                                        Session::Node { tx, .. } => Some((tx.clone(), origin_seq)),
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            if let Some((mut tx, origin_seq)) = pr {
                                tx.send(bus_message::Message::Rep {
                                    seq: origin_seq,
                                    result,
                                })
                                .await
                                .ok();
                            }
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
                                session_id,
                                from.map(|from| Cow::Owned(from.to_string()))
                                    .unwrap_or_else(|| Cow::Borrowed("<unknown>")),
                                to_service,
                                method
                            );

                            // 通知其它节点的指定服务
                            let bus = bus.lock().await;
                            let session = bus.sessions.get(session_id).unwrap();
                            let to = {
                                let current_node_id = match session {
                                    Session::Node { id, .. } => Some(*id),
                                    _ => None,
                                };
                                bus.nodes
                                    .iter()
                                    .filter(|(id, _)| match current_node_id {
                                        Some(current_node_id) => {
                                            current_node_id.to_u8() as usize != *id
                                        }
                                        _ => true,
                                    })
                                    .map(|(_, session_id)| {
                                        match bus.sessions.get(*session_id).unwrap() {
                                            Session::Client { tx } => tx.clone(),
                                            Session::Node { tx, .. } => tx.clone(),
                                        }
                                    })
                                    .collect::<Vec<_>>()
                            };

                            let from = match (session, from) {
                                (Session::Node { id, .. }, Some(lid)) => Some(lid.to_global(*id)),
                                _ => None,
                            };
                            for mut tx in to {
                                tx.send(bus_message::Message::Notify {
                                    from,
                                    to_service: to_service.clone(),
                                    method: method.clone(),
                                    data: data.clone(),
                                })
                                .await
                                .ok();
                            }
                        }
                        _ => return,
                    }
                }
            }
        };

        let hb_fut = {
            let bus = bus.clone();
            async move {
                loop {
                    task::sleep(Duration::from_secs(1)).await;
                    if let Session::Node { id, name, hb, .. } =
                        bus.lock().await.sessions.get(session_id).unwrap()
                    {
                        if hb.elapsed() > Duration::from_secs(30) {
                            // 心跳超时
                            trace!(
                                "[{}/MSG:HEARTBEAT_TIMEOUT] id={} name={}",
                                session_id,
                                id,
                                name
                            );
                            return;
                        }
                    }
                }
            }
        };

        let send_fut = {
            let stream = stream.clone();
            async move {
                while let Some(msg) = rx.next().await {
                    if let Err(_) = bus_message::write_message(&*stream, &msg).await {
                        break;
                    }
                }
            }
        };

        task::spawn({
            let bus = bus.clone();
            async move {
                select_all(vec![recv_fut.boxed(), hb_fut.boxed(), send_fut.boxed()]).await;

                let mut bus = bus.lock().await;

                if let Session::Node {
                    id: node_id, name, ..
                } = bus.sessions.get(session_id).unwrap()
                {
                    // 删除节点和服务信息
                    let node_id = *node_id;
                    let name = name.clone();
                    bus.nodes.remove(node_id.to_u8() as usize);
                    for (_, ids) in &mut bus.services {
                        ids.retain(|id| id.node_id() != node_id);
                    }

                    // 广播节点下线事件
                    for (_, session_id) in &bus.nodes {
                        let session = bus.sessions.get(*session_id).unwrap();
                        if let Session::Node { tx, .. } = session {
                            tx.clone()
                                .send(bus_message::Message::Event {
                                    event: Event::NodeUp {
                                        name: name.clone(),
                                        id: node_id,
                                    },
                                })
                                .await
                                .ok();
                        }
                    }
                }

                // 删除会话信息
                bus.sessions.remove(session_id);
            }
        });
    }

    Ok(())
}
