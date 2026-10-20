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
        self.check_request_replay(&accept.request_salt)
    }

    fn check_request_replay(&self, request_salt: &[u8]) -> Result<(), Error> {
        #[cfg(feature = "blake3")]
        {
            if self.cipher.is_blake3() && !request_salt.is_empty() {
                self.replay_pool.check_and_insert(request_salt)?;
            }
        }
        #[cfg(not(feature = "blake3"))]
        {
            let _ = request_salt;
        }
        Ok(())
    }
}

/// Protocol-owned TCP acceptor for one inbound listener.
///
/// This keeps Shadowsocks replay checks, session authentication metadata, and
/// AEAD stream construction inside the protocol crate. Proxy adapters only
/// pass the accepted socket in and receive destination/authentication facts plus
/// the original client stream. The old Session-returning API remains available.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksInboundTcpAcceptor {
    inbound: ShadowsocksInbound,
    profile: ShadowsocksInboundProfile,
    tcp_state: ShadowsocksInboundTcpState,
}

#[cfg(all(test, feature = "blake3"))]
#[path = "../../tests/inbound/facts.rs"]
mod facts_tests;

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
        stream: S,
    ) -> Result<(Session, crate::stream::ShadowsocksAeadStream<S>), Error>
    where
        S: zero_traits::AsyncSocket,
    {
        let (destination, auth, client) = self.accept_stream_facts(stream).await?;
        let mut session = Session::new(
            0,
            destination.address,
            destination.port,
            Network::Tcp,
            self.inbound.protocol(),
        );
        session.apply_auth(auth);
        Ok((session, client))
    }

    /// Authentication, replay admission and stream construction complete before
    /// the host allocates one business identity. This uses the listener's same
    /// original replay pools and the exact original cipher state/nonces/payload.
    pub async fn accept_stream_facts<S>(
        &self,
        mut stream: S,
    ) -> Result<
        (
            zero_core::request::Destination,
            SessionAuth,
            crate::stream::ShadowsocksAeadStream<S>,
        ),
        Error,
    >
    where
        S: zero_traits::AsyncSocket,
    {
        let (accept, user) = self
            .profile
            .accept_request_facts(&self.inbound, &mut stream)
            .await?;

        self.tcp_state.check_request_replay(&accept.request_salt)?;
        if !self.profile.is_2022() {
            self.profile.replay.check(&accept.request_salt)?;
        }

        let destination = accept.destination.clone();
        let auth = user.auth();
        let client = self.profile.build_request_stream(accept, &user, stream)?;
        Ok((destination, auth, client))
    }
}
