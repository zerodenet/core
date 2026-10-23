#![no_std]
#![allow(async_fn_in_trait)]

extern crate alloc;
#[cfg(feature = "runtime")]
extern crate std;

#[cfg(feature = "runtime")]
pub mod component;

mod body;
mod inbound;
mod metadata;
mod parse;
mod wire;

pub use body::{
    relay_close_delimited_as_chunked, relay_http_body, HttpBodyKind, HttpTransferCount,
};
pub use inbound::{
    HttpConnectInbound, HttpConnectResponse, HttpForwardCommand, HttpForwardRequest,
    HttpForwardResponse, HttpInboundCommand, HttpInboundMode, HttpInboundRequest,
};
pub use metadata::HttpConnectProtocol;
