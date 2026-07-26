pub use crate::inbound::TrojanInboundUserRef;

pub struct TrojanInboundOptionsRef<I> {
    pub users: I,
}

#[derive(Debug, Clone, Copy)]
pub struct TrojanOutboundOptionsRef<'a> {
    pub password: &'a str,
    pub sni: Option<&'a str>,
    pub insecure: bool,
    pub client_fingerprint: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct TrojanOutboundBuildOptionsRef<'a> {
    pub tag: &'a str,
    pub server: &'a str,
    pub port: u16,
    pub protocol: TrojanOutboundOptionsRef<'a>,
}
