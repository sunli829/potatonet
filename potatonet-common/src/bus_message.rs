use crate::{Event, LocalServiceId, NodeId, ServiceId};
use anyhow::Result;
use async_std::net::TcpStream;
use bytes::Bytes;
use futures::channel::mpsc::{Receiver, Sender};
use futures::prelude::*;
use std::io::Cursor;
use std::sync::Arc;

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
    SendNotify {
        from: LocalServiceId,
        to_service: String,
        method: u32,
        data: Bytes,
    },

    /// 客户端给指定服务发送通知
    SendNotifyTo {
        from: LocalServiceId,
        to: ServiceId,
        method: u32,
        data: Bytes,
    },

    /// 服务端发送通知
    Notify {
        from: ServiceId,
        to_service: String,
        method: u32,
        data: Bytes,
    },

    /// 服务端给指定服务发送通知
    NotifyTo {
        from: ServiceId,
        to: LocalServiceId,
        method: u32,
        data: Bytes,
    },

    /// 系统事件
    Event { event: Event },
}

async fn read_message<R: AsyncRead + Unpin>(mut r: R, buf: &mut Vec<u8>) -> Result<Message> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    buf.resize(u32::from_le_bytes(len) as usize, 0);
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
