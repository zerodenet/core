#![no_std]
#![allow(async_fn_in_trait)]

extern crate alloc;

mod body;
mod inbound;
mod metadata;
mod parse;
mod wire;

pub use body::{
    relay_close_delimited_as_chunked, relay_http_body, HttpBodyKind, HttpTransferCount,
};
pub use inbound::{
    HttpConnectInbound, HttpConnectResponse, HttpForwardRequest, HttpForwardResponse,
    HttpInboundMode, HttpInboundRequest,
};
pub use metadata::HttpConnectProtocol;
