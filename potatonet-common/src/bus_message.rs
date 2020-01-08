use crate::{Event, LocalServiceId, NodeId, ServiceId};
use anyhow::Result;
use bytes::Bytes;
use futures::prelude::*;
use std::io::Cursor;

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    /// 客户端发送ping
    Ping,

    /// 客户端注册节点
    RegisterNode(String),

    /// 服务端发送节点注册成功消息，并返回节点id
    NodeRegistered(NodeId),

    /// 客户端退出
    UnregisterNode,

    /// 注册服务
    RegisterService { name: String, id: LocalServiceId },

    /// 注销服务
    UnregisterService { id: LocalServiceId },

    /// 客户端发送请求
    Req {
        seq: u32,
        from: Option<LocalServiceId>,
        to_service: String,
        method: String,
        data: Bytes,
    },

    /// 服务端发送请求
    XReq {
        from: Option<ServiceId>,
        to: LocalServiceId,
        seq: u32,
        method: String,
        data: Bytes,
    },

    /// 服务端发送响应
    Rep {
        seq: u32,
        result: Result<Bytes, String>,
    },

    /// 客户端发送通知
    SendNotify {
        from: Option<LocalServiceId>,
        to_service: String,
        method: String,
        data: Bytes,
    },

    /// 服务端发送通知
    Notify {
        from: Option<ServiceId>,
        to_service: String,
        method: String,
        data: Bytes,
    },

    /// 系统事件
    Event { event: Event },
}

pub async fn read_message<R: AsyncRead + Unpin>(mut r: R) -> Result<Message> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let mut buf = Vec::with_capacity(u32::from_le_bytes(len) as usize);
    buf.resize(buf.capacity(), 0);
    r.read_exact(&mut buf).await?;
    let msg: Message = rmp_serde::from_read(Cursor::new(buf))?;
    Ok(msg)
}

pub async fn write_message<W: AsyncWrite + Unpin>(mut w: W, msg: &Message) -> Result<()> {
    let data = rmp_serde::to_vec(msg)?;
    w.write(&(data.len() as u32).to_le_bytes()).await?;
    w.write(&data).await?;
    Ok(())
}
