use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Cursor;

/// 请求类型
#[derive(Serialize, Deserialize, Clone)]
pub struct Request<T> {
    pub method: u32,
    pub data: T,
}

impl<T> Request<T> {
    pub fn new(method: u32, data: T) -> Self {
        Request {
            method: method.into(),
            data,
        }
    }
}

/// 响应类型
#[derive(Serialize, Deserialize)]
pub struct Response<T> {
    pub data: T,
}

impl<T> Response<T> {
    pub fn new(data: T) -> Self {
        Response { data }
    }
}

pub type RequestBytes = Request<Bytes>;

pub type ResponseBytes = Response<Bytes>;

impl<T: Serialize> Request<T> {
    pub fn to_bytes(&self) -> RequestBytes {
        RequestBytes {
            method: self.method.clone(),
            data: Bytes::from(rmp_serde::to_vec(&self.data).unwrap()),
        }
    }
}

impl<T: DeserializeOwned> Request<T> {
    pub fn from_bytes(req: RequestBytes) -> Self {
        Self {
            method: req.method,
            data: rmp_serde::from_read(Cursor::new(&req.data)).unwrap(),
        }
    }
}

impl<T: Serialize> Response<T> {
    pub fn to_bytes(&self) -> ResponseBytes {
        ResponseBytes {
            data: Bytes::from(rmp_serde::to_vec(&self.data).unwrap()),
        }
    }
}

impl<T: DeserializeOwned> Response<T> {
    pub fn from_bytes(req: ResponseBytes) -> Self {
        Self {
            data: rmp_serde::from_read(Cursor::new(&req.data)).unwrap(),
        }
    }
}
