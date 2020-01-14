use potatonet::*;

pub struct Echo;

#[service]
impl Echo {
    #[call]
    pub async fn send(&self, msg: String) -> String {
        msg
    }
}
