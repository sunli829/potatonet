#[macro_use]
extern crate anyhow;

pub use potatonet_common::*;

use async_std::net::{TcpStream, ToSocketAddrs};
use async_std::task;
use futures::channel::{mpsc, oneshot};
use futures::future::{AbortHandle, Abortable};
use futures::lock::Mutex;
use futures::{FutureExt, SinkExt, StreamExt};
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
    inner: Arc<Mutex<Inner>>,
    abort_handle: AbortHandle,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

impl Client {
    /// 连接到消息总线
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        let stream = Arc::new(TcpStream::connect(addr).await?);
        let (tx, mut rx) = mpsc::channel(16);
        let inner: Arc<Mutex<Inner>> = Default::default();

        // 消息发送
        let send_fut = {
            let stream = stream.clone();
            async move {
                while let Some(msg) = rx.next().await {
                    if let Err(_) = bus_message::write_message(&*stream, &msg).await {
                        return;
                    }
                }
            }
        };

        // 消息接收
        let recv_fut = {
            let stream = stream.clone();
            let inner = inner.clone();
            async move {
                loop {
                    let msg = match bus_message::read_message(&*stream).await {
                        Ok(msg) => msg,
                        Err(_) => return,
                    };

                    if let bus_message::Message::Rep { seq, result } = msg {
                        let mut inner = inner.lock().await;
                        if let Some(tx) = inner.pending.remove(&seq) {
                            tx.send(result.map(|data| Response::new(data))).ok();
                        }
                    }
                }
            }
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        task::spawn(
            Abortable::new(
                futures::future::select(send_fut.boxed(), recv_fut.boxed()),
                abort_registration,
            )
            .map(|_| ()),
        );
        Ok(Client {
            tx,
            inner,
            abort_handle,
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
                from: None,
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
                from: None,
                to_service: service_name.to_string(),
                method: request.method,
                data: request.data,
            })
            .await
            .ok();
    }
}
