use super::*;
#[cfg(feature = "crypto")]
#[derive(Clone, Copy)]
pub struct ShadowsocksTcpTarget<'a> {
    pub session: &'a Session,
    pub cipher: crate::shared::CipherKind,
    pub password: &'a [u8],
}

/// Parsed Shadowsocks TCP connect settings built from external config.
#[cfg(feature = "crypto")]
#[derive(Clone, PartialEq, Eq)]
pub struct ShadowsocksTcpConnectConfig {
    cipher: crate::shared::CipherKind,
    password: alloc::vec::Vec<u8>,
    response_password: alloc::vec::Vec<u8>,
    replay: crate::shared::legacy_replay::LegacyReplay,
}

#[cfg(feature = "crypto")]
impl ShadowsocksTcpConnectConfig {
    pub fn with_replay_policy(self, policy: crate::validation::ReplayPolicy) -> Self {
        self.with_replay_guard(crate::shared::legacy_replay::LegacyReplay::new(
            policy, false,
        ))
    }
    pub(crate) fn with_replay_guard(
        mut self,
        replay: crate::shared::legacy_replay::LegacyReplay,
    ) -> Self {
        self.replay = replay;
        self
    }

    pub fn from_config(cipher: &str, password: &str) -> Result<Self, Error> {
        let cipher = crate::shared::CipherKind::from_str(cipher)
            .ok_or(Error::Protocol("ss: unknown tcp cipher"))?;
        let password = password.as_bytes().to_vec();
        let response_password = if cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                crate::shared::parse_2022_key_chain(cipher, &password)?.user_password
            }
            #[cfg(not(feature = "blake3"))]
            {
                password.clone()
            }
        } else {
            password.clone()
        };
        Ok(Self {
            cipher,
            password,
            response_password,
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
        })
    }

    pub fn tcp_target<'a>(&'a self, session: &'a Session) -> ShadowsocksTcpTarget<'a> {
        ShadowsocksTcpTarget {
            session,
            cipher: self.cipher,
            password: &self.password,
        }
    }

    pub async fn establish_tcp_session<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<ShadowsocksOutboundSession, Error>
    where
        S: AsyncSocket,
    {
        if self.cipher.is_stream() {
            return crate::shared::legacy::send_request(
                stream,
                session,
                self.cipher,
                &self.password,
                Some(&self.replay),
            )
            .await;
        }
        if !self.cipher.is_blake3() {
            return ShadowsocksOutbound
                .send_request_legacy(
                    stream,
                    session,
                    self.cipher,
                    &self.password,
                    Some(&self.replay),
                )
                .await;
        }
        <ShadowsocksOutbound as TcpSessionProtocol<ShadowsocksTcpTarget<'_>>>::establish_tcp_session(
            &ShadowsocksOutbound,
            stream,
            &self.tcp_target(session),
        )
        .await
    }

    pub fn wrap_outbound_stream<S>(
        &self,
        stream: S,
        session: ShadowsocksOutboundSession,
    ) -> crate::stream::ShadowsocksAeadStream<S> {
        crate::stream::ShadowsocksAeadStream::outbound(
            stream,
            session,
            self.response_password.clone(),
        )
        .with_replay_guard(self.replay.clone())
    }
}

#[cfg(feature = "crypto")]
pub fn tcp_connect_config_from_config(
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksTcpConnectConfig, Error> {
    ShadowsocksTcpConnectConfig::from_config(cipher, password)
}

#[cfg(feature = "crypto")]
impl<'a> TcpSessionProtocol<ShadowsocksTcpTarget<'a>> for ShadowsocksOutbound {
    type Error = Error;
    type Session = ShadowsocksOutboundSession;

    async fn establish_tcp_session<S>(
        &self,
        stream: &mut S,
        target: &ShadowsocksTcpTarget<'a>,
    ) -> Result<Self::Session, Self::Error>
    where
        S: AsyncSocket,
    {
        self.send_request(stream, target.session, target.cipher, target.password)
            .await
    }
}

impl core::fmt::Debug for ShadowsocksTcpConnectConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksTcpConnectConfig")
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for ShadowsocksTcpTarget<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksTcpTarget")
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}
