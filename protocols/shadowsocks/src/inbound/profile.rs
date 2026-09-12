use super::*;
#[cfg(feature = "crypto")]
impl ShadowsocksInboundProfile {
    pub fn with_state_limits(mut self, limits: crate::validation::StateLimits) -> Self {
        self.limits = limits;
        self
    }
    pub(crate) fn state_limits(&self) -> crate::validation::StateLimits {
        self.limits
    }

    pub fn with_replay_policy(mut self, policy: crate::validation::ReplayPolicy) -> Self {
        self.replay = crate::shared::legacy_replay::LegacyReplay::new(policy, true);
        self
    }
    pub(crate) fn legacy_replay(&self) -> crate::shared::legacy_replay::LegacyReplay {
        self.replay.clone()
    }
    pub fn from_config(cipher_name: &str, password: &str) -> Result<Self, Error> {
        Self::from_config_users(
            cipher_name,
            [ShadowsocksInboundUserRef {
                password,
                principal_key: None,
                up_bps: None,
                down_bps: None,
                device_limit: None,
                quota_remaining_bytes: None,
                policy_revision: None,
            }],
        )
    }

    pub fn from_config_users<'a, I>(cipher_name: &str, users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = ShadowsocksInboundUserRef<'a>>,
    {
        Self::from_config_users_with_identity(cipher_name, None, users)
    }

    pub fn from_config_users_with_identity<'a, I>(
        cipher_name: &str,
        identity_password: Option<&str>,
        users: I,
    ) -> Result<Self, Error>
    where
        I: IntoIterator<Item = ShadowsocksInboundUserRef<'a>>,
    {
        let cipher = crate::shared::CipherKind::from_str(cipher_name)
            .ok_or(Error::Protocol("ss: unknown inbound cipher"))?;
        if identity_password.is_some()
            && !matches!(
                cipher,
                crate::shared::CipherKind::Blake3Aes128Gcm
                    | crate::shared::CipherKind::Blake3Aes256Gcm
            )
        {
            return Err(Error::Protocol(
                "ss: SIP023 EIH requires a 2022 AES inbound cipher",
            ));
        }
        let users = ShadowsocksAuthorizedUsers::from_refs(cipher, users)?;
        Ok(Self {
            cipher_name: String::from(cipher_name),
            cipher,
            identity_password: identity_password.map(|password| password.as_bytes().to_vec()),
            users: Arc::new(RwLock::new(Arc::new(users))),
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), true),
            limits: Default::default(),
        })
    }

    pub fn from_config_parts(cipher_name: &str, password: &str) -> Result<Self, Error> {
        Self::from_config(cipher_name, password)
    }

    pub fn from_config_cipher_password(cipher_name: &str, password: &str) -> Result<Self, Error> {
        Self::from_config_parts(cipher_name, password)
    }

    pub fn cipher_name(&self) -> &str {
        &self.cipher_name
    }

    pub fn user_count(&self) -> usize {
        self.users_snapshot().len()
    }

    pub fn replace_config_users<'a, I>(&self, users: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = ShadowsocksInboundUserRef<'a>>,
    {
        let users = ShadowsocksAuthorizedUsers::from_refs(self.cipher, users)?;
        *self
            .users
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Arc::new(users);
        Ok(())
    }

    pub(crate) fn users_snapshot(&self) -> Arc<ShadowsocksAuthorizedUsers> {
        self.users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub(crate) fn cipher(&self) -> crate::shared::CipherKind {
        self.cipher
    }

    pub(crate) fn identity_password(&self) -> Option<&[u8]> {
        self.identity_password.as_deref()
    }

    pub(crate) fn uses_eih(&self) -> bool {
        self.identity_password.is_some()
    }

    #[cfg(feature = "blake3")]
    pub(crate) fn identify_udp_user(&self, datagram: &[u8]) -> Result<ShadowsocksUser, Error> {
        let identity_password = self
            .identity_password()
            .ok_or(Error::Protocol("ss: SIP023 identity key is not configured"))?;
        let identity =
            crate::shared::identify_udp_2022_user(self.cipher, identity_password, datagram)?;
        self.users_snapshot()
            .find_identity(&identity)
            .map(|(_, user)| user.clone())
            .ok_or(Error::Protocol("ss: SIP023 udp user identity not found"))
    }

    pub fn is_2022(&self) -> bool {
        self.cipher.is_blake3()
    }

    pub fn tcp_state(&self) -> ShadowsocksInboundTcpState {
        ShadowsocksInboundTcpState::new(self.cipher, self.limits)
    }

    pub fn udp_codec(&self) -> ShadowsocksInboundUdpCodec {
        let users = self.users_snapshot();
        let password = users
            .first()
            .map(ShadowsocksUser::password)
            .unwrap_or_default();
        let codec = match self.identity_password() {
            Some(identity_password) => {
                ShadowsocksInboundUdpCodec::new_eih(self.cipher, identity_password, password)
            }
            None => ShadowsocksInboundUdpCodec::new(self.cipher, password)
                .with_replay_guard(self.replay.clone()),
        };
        codec.with_state_limits(self.limits)
    }

    pub fn udp_session(&self) -> ShadowsocksInboundUdpSession {
        ShadowsocksInboundUdpSession::new(self.udp_codec())
    }

    pub fn udp_responder(&self) -> ShadowsocksInboundUdpResponder {
        ShadowsocksInboundUdpResponder::new(self.udp_session())
    }

    pub fn accept_udp_session(&self) -> ShadowsocksInboundUdpResponder {
        self.udp_responder()
    }

    pub fn accept_udp_relay(&self) -> ShadowsocksInboundUdpRelay {
        ShadowsocksInboundUdpRelay::from_profile(self.clone())
    }

    pub fn into_listener_bindings(
        self,
    ) -> (ShadowsocksInboundTcpAcceptor, ShadowsocksInboundUdpRelay) {
        let udp_relay = self.accept_udp_relay();
        let tcp_acceptor = ShadowsocksInboundTcpAcceptor::new(self);
        (tcp_acceptor, udp_relay)
    }

    pub async fn accept_request<S: zero_traits::AsyncSocket>(
        &self,
        inbound: &ShadowsocksInbound,
        stream: &mut S,
    ) -> Result<(ShadowsocksAccept, ShadowsocksUser), Error> {
        let users = self.users_snapshot();
        let (accept, index) = inbound
            .accept_request_users(stream, self.cipher, self.identity_password(), &users)
            .await?;
        let user = users
            .get(index)
            .cloned()
            .ok_or(Error::Protocol("ss: matched inbound user disappeared"))?;
        Ok((accept, user))
    }

    pub fn into_aead_stream<S>(
        &self,
        accept: ShadowsocksAccept,
        user: &ShadowsocksUser,
        inner: S,
    ) -> Result<crate::stream::ShadowsocksAeadStream<S>, Error> {
        if !self.cipher.is_blake3() {
            let salt = self.replay.generate(self.cipher.salt_len())?;
            return accept.into_aead_stream_with_response_salt(inner, user.password(), salt);
        }
        accept.into_aead_stream(inner, user.password())
    }
}
