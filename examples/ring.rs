use async_std::task;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use potatonet::*;
use std::time::{Duration, Instant};

struct Node {
    next: String,
    tx: mpsc::Sender<()>,
}

#[message]
pub struct Msg {
    current: usize,
    stop: usize,
}

#[service]
impl Node {
    #[notify]
    pub async fn send(&self, ctx: &node::NodeContext<'_>, msg: Msg) {
        if msg.current == msg.stop {
            self.tx.clone().send(()).await.ok();
            return;
        }
        let client = NodeClient::with_name(ctx, &self.next);
        client
            .send(Msg {
                current: msg.current + 1,
                stop: msg.stop,
            })
            .await;
    }
}

#[async_std::main]
async fn main() {
    let bus_addr = "127.0.0.1:39901";

    let clap_app = clap::App::new("ring")
        .arg(
            clap::Arg::with_name("nodes")
                .short("n")
                .help("nodes count")
                .default_value("1"),
        )
        .arg(
            clap::Arg::with_name("services")
                .short("c")
                .help("services count for per node")
                .default_value("100"),
        )
        .arg(
            clap::Arg::with_name("value")
                .index(1)
                .help("stop value")
                .default_value("100000"),
        )
        .arg(
            clap::Arg::with_name("concurrency")
                .short("o")
                .help("number of multiple requests to make at a time")
                .default_value("1"),
        );
    let matches = clap_app.get_matches();

    // 启动消息总线
    task::spawn(potatonet_bus::run(bus_addr));
    task::sleep(Duration::from_secs(1)).await;

    // 启动节点
    let (tx, mut rx) = mpsc::channel(1);
    let mut id = 0;
    let nodes_count = matches.value_of("nodes").unwrap().parse().unwrap();
    let services_count = matches.value_of("services").unwrap().parse().unwrap();
    let stop_value = matches.value_of("value").unwrap().parse().unwrap();
    let concurrency = matches.value_of("concurrency").unwrap().parse().unwrap();
    let id_count = nodes_count * services_count;

    for _ in 0..nodes_count {
        let mut app = node::App::new();

        for _ in 0..services_count {
            let current = format!("node{}", id);
            let next_id = if id == id_count - 1 { 0 } else { id + 1 };
            let next = format!("node{}", next_id);
            id += 1;

            app = app.service_with_name(
                current,
                Node {
                    next,
                    tx: tx.clone(),
                },
            );
        }

        task::spawn(
            node::NodeBuilder::new(app)
                .bus_addr(bus_addr)
                .await
                .unwrap()
                .run(),
        );
    }
    task::sleep(Duration::from_secs(1)).await;

    // 开始发送
    let client = client::Client::connect(bus_addr).await.unwrap();
    let node_client = NodeClient::with_name(&client, "node0");

    let start = Instant::now();
    for _ in 0..concurrency {
        node_client
            .send(Msg {
                current: 0,
                stop: stop_value,
            })
            .await;
    }
    for _ in 0..concurrency {
        rx.next().await;
    }

    println!("nodes: {}", nodes_count);
    println!("services: {}", services_count);
    println!("stop value: {}", stop_value);
    println!("concurrency: {}", concurrency);
    println!(
        "QPS: {:.3}",
        (stop_value * concurrency) as f32 / start.elapsed().as_secs_f32()
    );
    println!("elapsed: {}s", start.elapsed().as_secs_f32());
    println!();
}
