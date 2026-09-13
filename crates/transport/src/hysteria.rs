//! Hysteria v2 as an authenticated byte-stream carrier (Xray v26.3.27).
//! The carried protocol owns all target-address and packet framing.
mod client;
mod dispatch;
mod masquerade;
mod request;
mod server;
mod settings;
pub use masquerade::Masquerade;
mod stream;
pub use client::{Client, Pool};
pub use server::{accept_connection, Incoming};
pub use settings::{Congestion, OptionsRef, Parameters};
use std::{io, sync::Arc};
pub use stream::HysteriaStream;

#[derive(Clone)]
pub struct Profile {
    auth: Arc<str>,
    masquerade: Masquerade,
    congestion: Congestion,
    uplink: u64,
    downlink: u64,
    quic: Parameters,
    hopping: Option<crate::datagram_hop::Profile>,
}
impl std::fmt::Debug for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HysteriaProfile").finish_non_exhaustive()
    }
}
impl Profile {
    pub fn with_masquerade(mut self, masquerade: Masquerade) -> Self {
        self.masquerade = masquerade;
        self
    }
    pub fn socket_factory(
        &self,
        factory: crate::OutboundDatagramSocketFactory,
    ) -> crate::OutboundDatagramSocketFactory {
        factory.with_hopping(self.hopping.clone())
    }
    pub fn cache_identity(&self) -> [u8; 32] {
        let identity = format!(
            "{}:{:?}:{}:{}:{:?}:{:?}",
            self.auth, self.congestion, self.uplink, self.downlink, self.quic, self.hopping
        );
        let digest = ring::digest::digest(&ring::digest::SHA256, identity.as_bytes());
        digest.as_ref().try_into().unwrap()
    }
    pub fn new(auth: &str) -> io::Result<Self> {
        if auth.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Hysteria carrier authentication is required",
            ));
        }
        http::HeaderValue::from_str(auth).map_err(io::Error::other)?;
        Ok(Self {
            auth: Arc::from(auth),
            masquerade: Masquerade::default(),
            congestion: Congestion::default(),
            uplink: 0,
            downlink: 0,
            quic: Parameters::default(),
            hopping: None,
        })
    }
}
fn padding() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let length = rng.random_range(256..2048);
    (0..length)
        .map(|_| {
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                [rng.random_range(0..62)] as char
        })
        .collect()
}

#[cfg(test)]
#[path = "../tests/hysteria/mod.rs"]
mod tests;
