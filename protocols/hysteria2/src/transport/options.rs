#[derive(Debug, Clone, Copy)]
pub struct Hysteria2InboundBindOptionsRef<'a> {
    pub cert_path: Option<&'a str>,
    pub key_path: Option<&'a str>,
}

pub use crate::inbound::Hysteria2InboundUserRef;

#[derive(Debug, Clone, Copy)]
pub struct Hysteria2InboundOptionsRef<I> {
    pub users: I,
}

#[derive(Debug, Clone, Copy)]
pub struct Hysteria2OutboundOptionsRef<'a> {
    pub password: &'a str,
    pub client_fingerprint: Option<&'a str>,
}
