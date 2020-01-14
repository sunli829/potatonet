use potatonet::client::*;

mod echoservice;

#[async_std::main]
async fn main() {
    let client = Client::connect("127.0.0.1:39901")
        .await
        .expect("failed to connect to bus");
    let echo_client = echoservice::EchoClient::new(&client);

    let mut rl = rustyline::Editor::<()>::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let res = echo_client
                    .send(line)
                    .await
                    .expect("failed to send message");
                println!("reply: {}", res);
            }
            Err(_) => return,
        }
    }
}
