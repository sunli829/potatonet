mod echoservice;

use async_std::task;
use potatonet::node::*;
use std::time::Duration;

#[async_std::main]
async fn main() {
    let bus_addr = "127.0.0.1:39901";

    // 启动消息总线
    task::spawn(potatonet_bus::run(bus_addr));
    task::sleep(Duration::from_secs(1)).await;

    // 启动节点
    NodeBuilder::new(App::new().service(echoservice::Echo))
        .bus_addr(bus_addr)
        .await
        .unwrap()
        .run()
        .await
        .unwrap();
}
