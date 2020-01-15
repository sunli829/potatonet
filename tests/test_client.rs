mod service;

use async_std::task;
use potatonet::client::*;
use potatonet::node::*;
use service::*;
use std::time::Duration;

#[async_std::test]
async fn test_service() {
    let bus_addr = "127.0.0.1:39901";

    // 启动消息总线
    task::spawn(potatonet_bus::run(bus_addr));
    task::sleep(Duration::from_secs(1)).await;

    // 启动两个节点
    task::spawn(
        NodeBuilder::new(App::new().service(TestService::default()))
            .bus_addr(bus_addr)
            .await
            .unwrap()
            .run(),
    );
    task::spawn(
        NodeBuilder::new(App::new().service(ProxyService))
            .bus_addr(bus_addr)
            .await
            .unwrap()
            .run(),
    );
    task::sleep(Duration::from_secs(1)).await;

    // TestService
    let client = Client::connect(bus_addr).await.unwrap();
    let testservice_client = TestServiceClient::new(&client);

    assert_eq!(testservice_client.add_one(10).await.unwrap(), 110);
    assert_eq!(testservice_client.add_one(50).await.unwrap(), 160);
    assert_eq!(
        testservice_client
            .add_one2(CustomMessage { value: 30 })
            .await
            .unwrap(),
        190
    );

    assert_eq!(testservice_client.add_two(30, 60).await.unwrap(), 280);
    assert_eq!(testservice_client.add_two2(11, 22).await.unwrap(), 313);

    testservice_client.notify_sub(5).await;
    task::sleep(Duration::from_secs(1)).await;
    assert_eq!(testservice_client.get().await.unwrap(), 308);

    testservice_client.notify_sub2(6, 12).await;
    task::sleep(Duration::from_secs(1)).await;
    assert_eq!(testservice_client.get().await.unwrap(), 290);

    // ProxyService
    let proxyservice_client = ProxyServiceClient::new(&client);
    assert_eq!(proxyservice_client.add_one(4).await.unwrap(), 294);
    assert_eq!(proxyservice_client.add_one(10).await.unwrap(), 304);
}
