//! Library factory backed by the actual native File grant; production stays gated.
#![cfg(feature = "vmess")]
mod fixtures;
mod rejected;
mod wire;

mod grpc;
mod preparation;

mod owned_grpc;
