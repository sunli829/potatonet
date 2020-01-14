use crate::{Backend, Item};
use bson::bson;
use mongodb::options::FindOptions;
use mongodb::{Client, Collection};
use potatonet_node::Result;

/// Mongodb日志后台配置
pub struct MongoBackendConfig {
    /// 连接字符串
    pub uri: String,

    /// 数据库名
    pub database: String,

    /// 表名
    pub collection: String,
}

/// Mongodb日志后台
pub struct MongoBackend {
    coll: Collection,
}

impl MongoBackend {
    // 新建Mongodb日志后台
    pub fn new(config: MongoBackendConfig) -> Result<Self> {
        let client = Client::with_uri_str(&config.uri)?;
        let db = client.database(&config.database);
        Ok(MongoBackend {
            coll: db.collection(&config.collection),
        })
    }
}

impl Backend for MongoBackend {
    fn append(&self, item: Item) -> Result<()> {
        if let bson::Bson::Document(doc) = bson::to_bson(&item)? {
            self.coll.insert_one(doc, None)?;
        }
        Ok(())
    }

    fn query_latest(&self, limit: usize) -> Result<Vec<Item>> {
        let mut cursor = self.coll.find(
            None,
            FindOptions {
                sort: Some(bson::doc! {"time": -1}),
                limit: Some(limit as i64),
                ..FindOptions::default()
            },
        )?;
        let mut items = Vec::new();
        while let Some(Ok(doc)) = cursor.next() {
            if let Ok(item) = bson::from_bson::<Item>(bson::Bson::Document(doc)) {
                items.push(item);
            }
        }
        Ok(items)
    }
}
