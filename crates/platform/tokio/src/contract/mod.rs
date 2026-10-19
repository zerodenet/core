//! Public runtime collaboration contracts. No configuration or kernel dependency.
pub mod component;
pub mod context;
pub mod datagram;
pub mod inbound;
pub mod lifecycle;
pub mod network;
pub mod packet;
pub mod scope;
pub mod stream;

pub use zero_error::channel::{
    ChannelError, CloseReason, DeliveryState, Impact, Reason, RetryClass, SendFailure, SendReceipt,
    Stage,
};

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
