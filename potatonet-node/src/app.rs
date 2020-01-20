use crate::service::{NamedService, Service};
use bytes::Bytes;
use potatonet_common::LocalServiceId;
use std::collections::HashMap;

pub type BoxService = Box<dyn Service<Req = Bytes, Rep = Bytes, Notify = Bytes>>;

/// 应用
pub struct App {
    /// 本地服务
    pub(crate) services: Vec<(String, BoxService)>,

    /// 服务名对应本地服务id
    pub(crate) services_map: HashMap<String, LocalServiceId>,
}

impl App {
    /// 创建应用
    pub fn new() -> Self {
        Self {
            services: Vec::new(),
            services_map: HashMap::new(),
        }
    }

    /// 添加服务
    pub fn service<S: NamedService + 'static>(self, service: S) -> Self {
        self.service_with_name(service.name().to_string(), service)
    }

    /// 添加服务并指定名称
    pub fn service_with_name<N: Into<String>, S: Service + 'static>(
        mut self,
        name: N,
        service: S,
    ) -> Self {
        let name = name.into();
        self.services.push((name.clone(), service.into()));
        self.services_map
            .insert(name, LocalServiceId((self.services.len() - 1) as u32));
        self
    }
}
