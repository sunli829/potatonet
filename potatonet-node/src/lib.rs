#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

mod app;
mod context;
mod node;
mod service;

#[doc(hidden)]
pub use async_trait;
#[doc(hidden)]
pub use serde_derive;

pub use app::App;
pub use context::NodeContext;
pub use node::NodeBuilder;
pub use potatonet_codegen::{message, service};
#[doc(hidden)]
pub use potatonet_common::*;
pub use service::{NamedService, Service};
