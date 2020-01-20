#![recursion_limit = "512"]

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

mod app;
mod context;
mod node;
mod service;

pub use app::App;
pub use context::NodeContext;
pub use node::NodeBuilder;
pub use potatonet_codegen::{message, service};
pub use service::{NamedService, Service};
