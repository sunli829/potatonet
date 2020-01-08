#[macro_use]
extern crate log;

use std::env;

#[async_std::main]
async fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let bind_addr = env::var("POTATONET_BUS_BIND").unwrap_or("127.0.0.1:39901".to_string());
    info!("server listen. addr={}", bind_addr);
    potatonet_bus::run(bind_addr).await.unwrap();
}
