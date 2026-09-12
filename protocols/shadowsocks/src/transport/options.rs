pub use crate::inbound::ShadowsocksInboundUserRef;

#[derive(Clone, Copy)]
pub struct ShadowsocksInboundOptionsRef<'a, I> {
    pub cipher: &'a str,
    pub identity_password: Option<&'a str>,
    pub users: I,
}

#[derive(Clone, Copy)]
pub struct ShadowsocksOutboundOptionsRef<'a> {
    pub cipher: &'a str,
    pub password: &'a str,
}

impl core::fmt::Debug for ShadowsocksOutboundOptionsRef<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksOutboundOptionsRef")
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}

impl<I> core::fmt::Debug for ShadowsocksInboundOptionsRef<'_, I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksInboundOptionsRef")
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}
