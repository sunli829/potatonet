use async_std::task;
use potatonet::*;
use std::time::{Duration, Instant};

const NODE_COUNT: usize = 100;
const MAX_VALUE: usize = 100000;

struct Node {
    next: String,
}

#[service]
impl Node {
    #[notify]
    pub async fn send(&self, ctx: &node::NodeContext<'_>, n: usize) {
        if n == MAX_VALUE {
            ctx.shutdown_node();
            return;
        }
        let client = NodeClient::with_name(ctx, &self.next);
        client.send(n + 1).await;
    }
}

#[async_std::main]
async fn main() {
    let bus_addr = "127.0.0.1:39901";

    // 启动消息总线
    task::spawn(potatonet_bus::run(bus_addr));
    task::sleep(Duration::from_secs(1)).await;

    // 启动NODE_COUNT个服务
    let mut app = node::App::new();
    for n in 0..NODE_COUNT {
        let current = format!("node{}", n);
        let next_id = if n == NODE_COUNT - 1 { 0 } else { n + 1 };
        let next = format!("node{}", next_id);
        app = app.service_with_name(current, Node { next });
    }

    // 启动节点
    let handle = task::spawn(
        node::NodeBuilder::new(app)
            .bus_addr(bus_addr)
            .await
            .unwrap()
            .run(),
    );
    task::sleep(Duration::from_secs(1)).await;

    // 开始发送
    let client = client::Client::connect(bus_addr).await.unwrap();
    let node_client = NodeClient::with_name(&client, "node0");

    let start = Instant::now();
    node_client.send(0).await;
    handle.await.unwrap();
    println!("{} {}s", MAX_VALUE, start.elapsed().as_secs_f32());
}
