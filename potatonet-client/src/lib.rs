#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::stream;
use async_std::task;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::prelude::*;
use futures::select;
use futures::{SinkExt, StreamExt};
use potatonet_common::bus_message::Message;
use potatonet_common::{
    bus_message, Context, Error, LocalServiceId, NodeId, Request, Response, ResponseBytes, Result,
    ServiceId, Topic,
};
use serde::de::DeserializeOwned;
use serde::export::PhantomData;
use serde::Serialize;
use slab::Slab;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[async_trait::async_trait]
trait SubCallback: Send {
    async fn notify(&mut self, data: &[u8]);
}

struct SubCallbackAdapter<
    T: Topic,
    F: FnMut(T) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
> {
    _market: PhantomData<T>,
    f: F,
}

#[async_trait::async_trait]
impl<T, F, R> SubCallback for SubCallbackAdapter<T, F, R>
where
    T: Topic,
    F: FnMut(T) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    async fn notify(&mut self, data: &[u8]) {
        if let Ok(msg) = T::decode(&data) {
            (self.f)(msg).await;
        }
    }
}

/// 订阅id
#[derive(Hash, Eq, PartialEq, Copy, Clone)]
pub struct SubscribeId(usize);

#[derive(Default)]
struct Inner {
    pending: HashMap<u32, oneshot::Sender<std::result::Result<ResponseBytes, String>>>,
    seq: u32,
    subscribes_set: HashMap<String, usize>,
    subscribes: Slab<(String, Box<dyn SubCallback>)>,
}

/// 客户端
pub struct Client {
    node_id: NodeId,
    tx: mpsc::Sender<bus_message::Message>,
    tx_abort: mpsc::Sender<()>,
    inner: Arc<Mutex<Inner>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.tx_abort.try_send(()).ok();
        info!("client closed");
    }
}

impl Client {
    async fn process_incoming_msg(inner: Arc<Mutex<Inner>>, msg: &bus_message::Message) {
        match msg {
            bus_message::Message::Rep { seq, result } => {
                let mut inner = inner.lock().await;
                if let Some(tx) = inner.pending.remove(&seq) {
                    tx.send(result.clone().map(|data| Response::new(data))).ok();
                }
            }
            bus_message::Message::XPublish { topic, data } => {
                let mut inner = inner.lock().await;
                for (_, (sub_topic, f)) in &mut inner.subscribes {
                    if topic == sub_topic.as_str() {
                        f.notify(&data).await;
                    }
                }
            }
            _ => {}
        }
    }

    #[doc(hidden)]
    pub async fn connect_with_notify<A, F, R>(addr: A, mut handle_msg: F) -> Result<Client>
    where
        A: ToSocketAddrs,
        F: FnMut(bus_message::Message) -> R + Send + Sync + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        let addr = addr.to_socket_addrs().await?.next();
        let stream = match addr {
            Some(addr) => {
                info!("connect to bus. addr={}", addr);
                Arc::new(TcpStream::connect(addr).await?)
            }
            None => bail!("could not resolve to any addresses"),
        };

        // 等待hello消息
        let node_id = match potatonet_common::bus_message::read_one_message(&*stream).await {
            Ok(bus_message::Message::Hello(node_id)) => node_id,
            Ok(msg) => {
                println!("{:?}", msg);
                bail!("invalid response")
            }
            Err(err) => return Err(err),
        };

        let inner: Arc<Mutex<Inner>> = Default::default();

        // 消息发送
        let (tx_incoming_msg, mut rx_incoming_msg) = mpsc::channel(16);
        let (tx_outgoing_msg, rx_outgoing_msg) = mpsc::channel(16);

        let (reader_task, abort_reader) =
            future::abortable(bus_message::read_messages(stream.clone(), tx_incoming_msg));
        let reader_handle = task::spawn(reader_task);

        let (writer_task, abort_writer) =
            future::abortable(bus_message::write_messages(stream.clone(), rx_outgoing_msg));
        let writer_handle = task::spawn(writer_task);

        let (tx_abort, mut rx_abort) = mpsc::channel::<()>(1);

        let fut = {
            let inner = inner.clone();
            let mut tx_outgoing_msg = tx_outgoing_msg.clone();
            async move {
                // 心跳发送定时器
                let mut hb_timer = stream::interval(Duration::from_secs(1)).fuse();

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
                                Self::process_incoming_msg(inner.clone(), &msg).await;
                                handle_msg(msg).await;
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

                info!("client closed");
            }
        };
        task::spawn(fut);

        Ok(Client {
            node_id,
            tx: tx_outgoing_msg,
            tx_abort,
            inner,
        })
    }

    /// 连接到消息总线
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        Self::connect_with_notify(addr, |_| async move {}).await
    }

    /// 获取当前节点id
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// 注册服务
    pub async fn register_service<N: Into<String>>(&self, name: N, id: LocalServiceId) {
        self.tx
            .clone()
            .send(bus_message::Message::RegisterService {
                name: name.into(),
                id,
            })
            .await
            .ok();
    }

    /// 注销服务
    pub async fn unregister_service(&self, id: LocalServiceId) {
        self.tx
            .clone()
            .send(bus_message::Message::UnregisterService { id })
            .await
            .ok();
    }

    #[doc(hidden)]
    pub async fn send_msg(&self, msg: Message) {
        self.tx.clone().send(msg).await.ok();
    }

    /// 订阅消息
    /// 如果重复订阅，则会按订阅顺序依次触发回调函数，并不会增加数据传输流量
    pub async fn subscribe<T, F, R>(&self, handler: F) -> SubscribeId
    where
        T: Topic,
        F: FnMut(T) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.subscribe_with_topic(T::name(), handler).await
    }

    /// 订阅指定主题的消息
    pub async fn subscribe_with_topic<T, F, R>(&self, topic: &str, handler: F) -> SubscribeId
    where
        T: Topic,
        F: FnMut(T) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().await;
        let id = inner.subscribes.insert((
            topic.to_string(),
            Box::new(SubCallbackAdapter {
                _market: PhantomData,
                f: handler,
            }),
        ));
        if let Some(count) = inner.subscribes_set.get_mut(topic) {
            *count += 1;
        } else {
            // 第一次订阅
            inner.subscribes_set.insert(topic.to_string(), 1);
            self.send_msg(bus_message::Message::Subscribe {
                topic: topic.to_string(),
            })
            .await;
        }
        SubscribeId(id)
    }

    /// 取消订阅消息
    pub async fn unsubscribe(&self, id: SubscribeId) {
        let mut inner = self.inner.lock().await;
        if inner.subscribes.contains(id.0 as usize) {
            let (topic, _) = inner.subscribes.remove(id.0 as usize);
            self.send_msg(bus_message::Message::Unsubscribe {
                topic: topic.to_string(),
            })
            .await;
            if let Some(count) = inner.subscribes_set.get_mut(&topic) {
                *count -= 1;
                if *count == 0 {
                    inner.subscribes_set.remove(&topic);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Context for Client {
    async fn call<T, R>(&self, service_name: &str, request: Request<T>) -> Result<Response<R>>
    where
        T: Serialize + Send + 'static,
        R: DeserializeOwned + Send + 'static,
    {
        let (seq, rx) = {
            let mut inner = self.inner.lock().await;
            let (tx, rx) = oneshot::channel();

            inner.seq += 1;
            let seq = inner.seq;
            inner.pending.insert(seq, tx);
            (seq, rx)
        };

        let request = request.to_bytes();
        self.tx
            .clone()
            .send(bus_message::Message::Req {
                seq,
                from: LocalServiceId(0),
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
                let mut inner = self.inner.lock().await;
                inner.pending.remove(&seq);
                Err(Error::Internal.into())
            }
            Err(_) => {
                let mut inner = self.inner.lock().await;
                inner.pending.remove(&seq);
                Err(Error::Timeout.into())
            }
        }
    }

    async fn notify<T: Serialize + Send + 'static>(&self, service_name: &str, request: Request<T>) {
        let request = request.to_bytes();
        self.tx
            .clone()
            .send(bus_message::Message::Notify {
                from: LocalServiceId(0),
                to_service: service_name.to_string(),
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }

    async fn notify_to<T: Serialize + Send + 'static>(&self, to: ServiceId, request: Request<T>) {
        let request = request.to_bytes();
        self.tx
            .clone()
            .send(bus_message::Message::NotifyTo {
                from: LocalServiceId(0),
                to,
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }

    async fn publish_with_topic<T: Topic>(&self, topic: &str, msg: T) {
        if let Ok(data) = msg.encode() {
            self.tx
                .clone()
                .send(bus_message::Message::Publish {
                    topic: topic.to_string(),
                    data: data.into(),
                })
                .await
                .ok();
        }
    }
}
