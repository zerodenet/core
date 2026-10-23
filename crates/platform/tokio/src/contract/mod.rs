//! Public runtime collaboration contracts. No configuration or kernel dependency.
pub mod admission;
pub mod component;
pub mod context;
pub mod datagram;
pub mod dns;
pub mod inbound;
pub mod lifecycle;
pub mod network;
pub mod packet;
pub mod scope;
pub mod service;
pub mod stream;

pub use tokio_util::sync::CancellationToken;

pub use zero_error::channel::{
    ChannelError, CloseReason, DeliveryState, Impact, Reason, RetryClass, SendFailure, SendReceipt,
    Stage,
};

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
