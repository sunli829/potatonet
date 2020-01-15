use linefeed::{Interface, ReadResult};
use potatonet::client::*;

mod echoservice;

#[async_std::main]
async fn main() {
    let client = Client::connect("127.0.0.1:39901")
        .await
        .expect("failed to connect to bus");
    let echo_client = echoservice::EchoClient::new(&client);

    let reader = Interface::new("echo client").unwrap();
    reader.set_prompt(">> ").unwrap();

    while let ReadResult::Input(input) = reader.read_line().unwrap() {
        let res = echo_client
            .send(input)
            .await
            .expect("failed to send message");
        println!("reply: {}", res);
    }
}
