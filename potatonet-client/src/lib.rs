#[macro_use]
extern crate anyhow;

use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::stream;
use async_std::task;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::prelude::*;
use futures::select;
use futures::{SinkExt, StreamExt};
use potatonet_common::{
    bus_message, Context, Error, LocalServiceId, Request, Response, ResponseBytes, Result,
    ServiceId,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct Inner {
    pending: HashMap<u32, oneshot::Sender<std::result::Result<ResponseBytes, String>>>,
    seq: u32,
}

/// 客户端
pub struct Client {
    tx: mpsc::Sender<bus_message::Message>,
    tx_abort: mpsc::Sender<()>,
    inner: Arc<Mutex<Inner>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.tx_abort.start_send(()).ok();
    }
}

impl Client {
    async fn process_incoming_msg(inner: Arc<Mutex<Inner>>, msg: bus_message::Message) {
        match msg {
            bus_message::Message::Rep { seq, result } => {
                let mut inner = inner.lock().await;
                if let Some(tx) = inner.pending.remove(&seq) {
                    tx.send(result.map(|data| Response::new(data))).ok();
                }
            }
            _ => {}
        }
    }

    /// 连接到消息总线
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        let stream = Arc::new(TcpStream::connect(addr).await?);
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
                                Self::process_incoming_msg(inner.clone(), msg).await;
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
            }
        };
        task::spawn(fut);

        Ok(Client {
            tx: tx_outgoing_msg,
            tx_abort,
            inner,
        })
    }
}

#[async_trait::async_trait]
impl Context for Client {
    async fn call<T, R>(&self, service_name: &str, request: Request<T>) -> Result<Response<R>>
    where
        T: Serialize + Send + Sync,
        R: DeserializeOwned + Send + Sync,
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

    async fn notify<T: Serialize + Send + Sync>(&self, service_name: &str, request: Request<T>) {
        let request = request.to_bytes();
        self.tx
            .clone()
            .send(bus_message::Message::SendNotify {
                from: LocalServiceId(0),
                to_service: service_name.to_string(),
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }

    async fn notify_to<T: Serialize + Send + Sync>(&self, to: ServiceId, request: Request<T>) {
        let request = request.to_bytes();
        self.tx
            .clone()
            .send(bus_message::Message::SendNotifyTo {
                from: LocalServiceId(0),
                to,
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }
}
