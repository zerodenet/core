// REALITY protocol implementation — uses shared `ztls` crate for TLS 1.3 primitives.

pub mod reality_auth;
pub mod reality_client_connection;
pub mod reality_client_verify;
pub mod reality_server_connection;
pub mod reality_util;
pub mod stream;

pub use stream::{
    generate_reality_key_pair, upgrade_reality_client, upgrade_reality_server,
    RealityClientOptions, RealityServerOptions, RealityTlsStream, VlessRealityServerProfile,
};

pub mod mldsa;

mod hello;

pub mod target;

mod hybrid;
