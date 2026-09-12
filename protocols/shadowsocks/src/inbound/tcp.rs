use super::*;
/// Listener-scoped Shadowsocks TCP state.
///
/// The proxy keeps this value with its inbound handler and delegates
/// protocol-private replay checks to it.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksInboundTcpState {
    #[cfg(feature = "blake3")]
    cipher: crate::shared::CipherKind,
    #[cfg(feature = "blake3")]
    replay_pool: Arc<crate::shared::ReplaySaltPool>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundTcpState {
    pub(super) fn new(
        cipher: crate::shared::CipherKind,
        limits: crate::validation::StateLimits,
    ) -> Self {
        #[cfg(not(feature = "blake3"))]
        let _ = (cipher, limits);
        Self {
            #[cfg(feature = "blake3")]
            cipher,
            #[cfg(feature = "blake3")]
            replay_pool: Arc::new(crate::shared::ReplaySaltPool::configured(
                crate::shared::ReplaySaltPool::DEFAULT_TTL,
                limits.tcp_replay_capacity,
            )),
        }
    }

    pub fn check_accept_replay(&self, accept: &ShadowsocksAccept) -> Result<(), Error> {
        #[cfg(feature = "blake3")]
        {
            if self.cipher.is_blake3() && !accept.request_salt.is_empty() {
                self.replay_pool.check_and_insert(&accept.request_salt)?;
            }
        }
        #[cfg(not(feature = "blake3"))]
        {
            let _ = accept;
        }
        Ok(())
    }
}

/// Protocol-owned TCP acceptor for one inbound listener.
///
/// This keeps Shadowsocks replay checks, session authentication metadata, and
/// AEAD stream construction inside the protocol crate. Proxy adapters only
/// pass the accepted socket in and receive the neutral session plus client
/// stream back.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksInboundTcpAcceptor {
    inbound: ShadowsocksInbound,
    profile: ShadowsocksInboundProfile,
    tcp_state: ShadowsocksInboundTcpState,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundTcpAcceptor {
    pub fn new(profile: ShadowsocksInboundProfile) -> Self {
        let tcp_state = profile.tcp_state();
        Self {
            inbound: ShadowsocksInbound,
            profile,
            tcp_state,
        }
    }

    pub async fn accept_stream<S>(
        &self,
        mut stream: S,
    ) -> Result<(Session, crate::stream::ShadowsocksAeadStream<S>), Error>
    where
        S: zero_traits::AsyncSocket,
    {
        let (accept, user) = self
            .profile
            .accept_request(&self.inbound, &mut stream)
            .await?;

        self.tcp_state.check_accept_replay(&accept)?;
        if !self.profile.is_2022() {
            self.profile.replay.check(&accept.request_salt)?;
        }

        let mut session = accept.session.clone();
        session.apply_auth(user.auth());

        let client = self.profile.into_aead_stream(accept, &user, stream)?;

        Ok((session, client))
    }
}
