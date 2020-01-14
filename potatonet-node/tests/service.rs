use potatonet_node::*;
use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Default)]
pub struct TestService {
    sum: AtomicI32,
}

#[message]
pub struct CustomMessage {
    pub value: i32,
}

#[service]
impl TestService {
    async fn init(&self, _ctx: &NodeContext<'_>) {
        self.sum.store(100, Ordering::Relaxed);
    }

    async fn shutdown(&self, _ctx: &NodeContext<'_>) {
        self.sum.store(0, Ordering::Relaxed);
    }

    // 内部方法
    fn add(&self, a: i32, b: i32) -> i32 {
        a + b
    }

    #[call]
    async fn get(&self) -> i32 {
        self.sum.load(Ordering::Relaxed)
    }

    #[call]
    async fn add_one(&self, n: i32) -> Result<i32> {
        self.sum.fetch_add(n, Ordering::Relaxed);
        Ok(self.sum.load(Ordering::Relaxed))
    }

    #[call]
    async fn add_one2(&self, msg: CustomMessage) -> Result<i32> {
        self.sum.fetch_add(msg.value, Ordering::Relaxed);
        Ok(self.sum.load(Ordering::Relaxed))
    }

    #[call]
    async fn add_two(&self, a: i32, b: i32) -> i32 {
        self.sum.fetch_add(self.add(a, b), Ordering::Relaxed);
        self.sum.load(Ordering::Relaxed)
    }

    #[call]
    async fn add_two2(&self, a: i32, b: i32) -> Result<i32> {
        self.sum.fetch_add(self.add(a, b), Ordering::Relaxed);
        Ok(self.sum.load(Ordering::Relaxed))
    }

    #[notify]
    async fn notify_sub(&self, n: i32) {
        self.sum.fetch_sub(n, Ordering::Relaxed);
    }

    #[notify]
    async fn notify_sub2(&self, a: i32, b: i32) {
        self.sum.fetch_sub(self.add(a, b), Ordering::Relaxed);
    }
}

pub struct ProxyService;

#[service]
impl ProxyService {
    #[call]
    async fn add_one(&self, ctx: &NodeContext<'_>, n: i32) -> Result<i32> {
        let client = TestServiceClient::new(ctx);
        client.add_one(n).await
    }
}
