use crate::{LocalServiceId, NodeId, ServiceId};
use anyhow::Result;
use async_std::net::TcpStream;
use bytes::Bytes;
use futures::channel::mpsc::{Receiver, Sender};
use futures::prelude::*;
use std::io::Cursor;
use std::sync::Arc;

const MAX_DATA_SIZE: usize = 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    /// 客户端发送ping
    Ping,

    /// 客户端断开连接
    Bye,

    /// 服务端发送欢迎消息
    Hello(NodeId),

    /// 注册服务
    RegisterService { name: String, id: LocalServiceId },

    /// 注销服务
    UnregisterService { id: LocalServiceId },

    /// 客户端发送请求
    Req {
        seq: u32,
        from: LocalServiceId,
        to_service: String,
        method: u32,
        data: Bytes,
    },

    /// 服务端发送请求
    XReq {
        from: ServiceId,
        to: LocalServiceId,
        seq: u32,
        method: u32,
        data: Bytes,
    },

    /// 服务端发送响应
    Rep {
        seq: u32,
        result: Result<Bytes, String>,
    },

    /// 客户端发送通知
    Notify {
        from: LocalServiceId,
        to_service: String,
        method: u32,
        data: Bytes,
    },

    /// 客户端给指定服务发送通知
    NotifyTo {
        from: LocalServiceId,
        to: ServiceId,
        method: u32,
        data: Bytes,
    },

    /// 服务端发送通知
    XNotify {
        from: ServiceId,
        to_service: String,
        method: u32,
        data: Bytes,
    },

    /// 服务端给指定服务发送通知
    XNotifyTo {
        from: ServiceId,
        to: LocalServiceId,
        method: u32,
        data: Bytes,
    },

    /// 客户端订阅消息请求
    Subscribe { topic: String },

    /// 客户端取消订阅消息请求
    Unsubscribe { topic: String },

    /// 客户端发布消息
    Publish { topic: String, data: Bytes },

    /// 服务器发布消息
    XPublish { topic: String, data: Bytes },
}

async fn read_message<R: AsyncRead + Unpin>(mut r: R, buf: &mut Vec<u8>) -> Result<Message> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let data_size = u32::from_le_bytes(len) as usize;
    if data_size > MAX_DATA_SIZE {
        bail!("data length exceeding the limit");
    }
    buf.resize(data_size, 0);
    r.read_exact(buf).await?;
    let msg: Message = rmp_serde::from_read(Cursor::new(&buf))?;
    Ok(msg)
}

async fn write_message<W: AsyncWrite + Unpin>(
    mut w: W,
    msg: &Message,
    buf: &mut Vec<u8>,
) -> Result<()> {
    buf.clear();
    rmp_serde::encode::write(buf, &msg)?;
    w.write(&(buf.len() as u32).to_le_bytes()).await?;
    w.write(&buf).await?;
    Ok(())
}

pub async fn read_one_message<R: AsyncRead + Unpin>(r: R) -> Result<Message> {
    let mut buf = Vec::new();
    read_message(r, &mut buf).await
}

pub async fn read_messages(stream: Arc<TcpStream>, mut tx: Sender<Message>) {
    let mut buf = Vec::with_capacity(1024);
    while let Ok(msg) = read_message(&*stream, &mut buf).await {
        if let Err(_) = tx.send(msg).await {
            // 连接已断开
            break;
        }
    }
}

pub async fn write_messages(stream: Arc<TcpStream>, mut rx: Receiver<Message>) {
    let mut buf = Vec::with_capacity(1024);

    while let Some(msg) = rx.next().await {
        if let Err(_) = write_message(&*stream, &msg, &mut buf).await {
            // 连接已断开
            break;
        }
    }
}
