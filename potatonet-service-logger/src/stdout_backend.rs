use crate::{Backend, Item};
use potatonet::*;
use std::fmt::Write;

/// 标准输出日志后台
pub struct StdoutBackend;

impl Backend for StdoutBackend {
    fn append(&self, item: Item) -> Result<()> {
        let mut buf = String::new();
        write!(
            buf,
            "[{}] {} {}",
            item.level.as_str(),
            item.time.format("%Y-%m-%d %H:%M:%S"),
            item.message,
        )?;
        if let Some(kvs) = item.kvs {
            write!(buf, " ")?;

            for (k, v) in kvs {
                write!(buf, "{} = {}", k, v)?;
            }
        }
        println!("{}", buf);
        Ok(())
    }

    fn query_latest(&self, _limit: usize) -> Result<Vec<Item>> {
        Ok(Default::default())
    }
}
