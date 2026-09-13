use alloc::string::String;

/// A local stream endpoint selected by the owning inbound protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FallbackEndpoint {
    Tcp { server: String, port: u16 },
    Unix { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackRule {
    pub name: String,
    pub alpn: String,
    pub path: String,
    pub endpoint: FallbackEndpoint,
    pub proxy_protocol: u8,
}

/// Selection is protocol-owned; dialing and optional PROXY handoff are neutral.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackRoute {
    pub endpoint: FallbackEndpoint,
    pub proxy_protocol: u8,
    pub source: Option<core::net::SocketAddr>,
    pub destination: Option<core::net::SocketAddr>,
}
