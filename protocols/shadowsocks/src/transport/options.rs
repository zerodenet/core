pub use crate::inbound::ShadowsocksInboundUserRef;

#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksInboundOptionsRef<'a, I> {
    pub cipher: &'a str,
    pub identity_password: Option<&'a str>,
    pub users: I,
}

#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksOutboundOptionsRef<'a> {
    pub cipher: &'a str,
    pub password: &'a str,
}
