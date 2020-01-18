use crate::service::{DynService, NamedService, Service, ServiceAdapter};
use potatonet_common::LocalServiceId;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;

/// 应用
pub struct App {
    /// 本地服务
    pub(crate) services: Vec<(String, Box<DynService>)>,

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
    pub fn service<S>(self, service: S) -> Self
    where
        S: NamedService + 'static,
        S::Req: Serialize + DeserializeOwned + Send,
        S::Rep: Serialize + DeserializeOwned + Send,
        S::Notify: Serialize + DeserializeOwned + Send,
    {
        self.service_with_name(service.name().to_string(), service)
    }

    /// 添加服务并指定名称
    pub fn service_with_name<N, S>(mut self, name: N, service: S) -> Self
    where
        N: Into<String>,
        S: Service + 'static,
        S::Req: Serialize + DeserializeOwned + Send,
        S::Rep: Serialize + DeserializeOwned + Send,
        S::Notify: Serialize + DeserializeOwned + Send,
    {
        let name = name.into();
        self.services
            .push((name.clone(), Box::new(ServiceAdapter(service))));
        self.services_map
            .insert(name, LocalServiceId((self.services.len() - 1) as u32));
        self
    }
}
