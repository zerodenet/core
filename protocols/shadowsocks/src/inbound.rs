// Shadowsocks inbound protocol.

#[cfg(feature = "crypto")]
use alloc::string::String;
#[cfg(feature = "crypto")]
use alloc::sync::Arc;
#[cfg(feature = "crypto")]
use alloc::vec::Vec;
#[cfg(feature = "crypto")]
use std::{
    collections::HashMap,
    sync::{Mutex, RwLock},
};
use zero_core::ProtocolType;
#[cfg(feature = "crypto")]
use zero_core::{Error, Network, Session, SessionAuth};

#[cfg(feature = "crypto")]
use crate::udp::{
    ShadowsocksInboundUdpCodec, ShadowsocksInboundUdpRelay, ShadowsocksInboundUdpResponder,
    ShadowsocksInboundUdpSession,
};

/// Shadowsocks inbound handler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShadowsocksInbound;

/// Result of accepting a Shadowsocks TCP connection.
#[cfg(feature = "crypto")]
pub struct ShadowsocksAccept {
    pub session: Session,
    /// Remaining plaintext payload after the target address in the first chunk.
    pub remaining_payload: Vec<u8>,
    /// Derived session key for subsequent AEAD operations.
    pub session_key: Vec<u8>,
    /// Cipher kind for subsequent chunks.
    pub cipher: super::shared::CipherKind,
    /// Next nonce for decrypting client-to-server chunks after the first request chunk.
    pub next_upload_nonce: u64,
    /// For 2022 edition: the client request salt, echoed back in the server
    /// response fixed header. Empty for legacy AEAD.
    pub request_salt: Vec<u8>,
}

/// Protocol-owned validated inbound profile.
///
/// Proxy runtime code keeps this as an opaque profile and delegates TCP/UDP
/// Shadowsocks framing decisions back to the protocol crate.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksInboundProfile {
    cipher_name: String,
    cipher: super::shared::CipherKind,
    identity_password: Option<Vec<u8>>,
    users: Arc<RwLock<Arc<ShadowsocksAuthorizedUsers>>>,
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUser {
    password: Vec<u8>,
    principal_key: Option<String>,
    up_bps: Option<u64>,
    down_bps: Option<u64>,
    device_limit: Option<u32>,
    quota_remaining_bytes: Option<u64>,
    policy_revision: Option<u64>,
}

#[cfg(feature = "crypto")]
#[derive(Debug)]
pub(crate) struct ShadowsocksAuthorizedUsers {
    users: Arc<[ShadowsocksUser]>,
    #[cfg(feature = "blake3")]
    identities: HashMap<[u8; 16], usize>,
}

#[cfg(feature = "crypto")]
impl core::ops::Deref for ShadowsocksAuthorizedUsers {
    type Target = [ShadowsocksUser];

    fn deref(&self) -> &Self::Target {
        &self.users
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksInboundUserRef<'a> {
    pub password: &'a str,
    pub principal_key: Option<&'a str>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

#[cfg(feature = "crypto")]
impl core::fmt::Debug for ShadowsocksInboundProfile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ShadowsocksInboundProfile")
            .field("cipher_name", &self.cipher_name)
            .field("user_count", &self.user_count())
            .finish()
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksUser {
    fn from_ref(user: ShadowsocksInboundUserRef<'_>) -> Self {
        Self {
            password: user.password.as_bytes().to_vec(),
            principal_key: user.principal_key.map(String::from),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        }
    }

    pub(crate) fn password(&self) -> &[u8] {
        &self.password
    }

    pub(crate) fn cache_key(&self) -> String {
        String::from_utf8_lossy(&self.password).to_string()
    }

    pub(crate) fn auth(&self) -> SessionAuth {
        let mut auth = SessionAuth::new("shadowsocks");
        auth.principal_key = self
            .principal_key
            .clone()
            .or_else(|| Some(self.cache_key()));
        auth.up_bps = self.up_bps;
        auth.down_bps = self.down_bps;
        auth.device_limit = self.device_limit;
        auth.quota_remaining_bytes = self.quota_remaining_bytes;
        auth.policy_revision = self.policy_revision;
        auth
    }

    #[cfg(feature = "blake3")]
    fn identity_hash(&self, cipher: super::shared::CipherKind) -> Result<[u8; 16], Error> {
        super::shared::identity_hash_2022(cipher, &self.password)
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksAuthorizedUsers {
    fn from_refs<'a, I>(cipher: super::shared::CipherKind, users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = ShadowsocksInboundUserRef<'a>>,
    {
        let users: Arc<[ShadowsocksUser]> = users
            .into_iter()
            .map(ShadowsocksUser::from_ref)
            .collect::<Vec<_>>()
            .into();
        #[cfg(feature = "blake3")]
        let identities = if cipher.is_blake3() {
            let mut identities = HashMap::with_capacity(users.len());
            for (index, user) in users.iter().enumerate() {
                let identity = user.identity_hash(cipher)?;
                if identities.insert(identity, index).is_some() {
                    return Err(Error::Protocol("ss: duplicate SIP023 user identity"));
                }
            }
            identities
        } else {
            HashMap::new()
        };
        Ok(Self {
            users,
            #[cfg(feature = "blake3")]
            identities,
        })
    }

    #[cfg(feature = "blake3")]
    pub(crate) fn find_identity(&self, identity: &[u8; 16]) -> Option<(usize, &ShadowsocksUser)> {
        let index = *self.identities.get(identity)?;
        self.users.get(index).map(|user| (index, user))
    }
}

/// Listener-scoped Shadowsocks TCP state.
///
/// The proxy keeps this value with its inbound handler and delegates
/// protocol-private replay checks to it.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksInboundTcpState {
    cipher: super::shared::CipherKind,
    #[cfg(feature = "blake3")]
    replay_pool: Arc<super::shared::ReplaySaltPool>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundTcpState {
    fn new(cipher: super::shared::CipherKind) -> Self {
        Self {
            cipher,
            #[cfg(feature = "blake3")]
            replay_pool: Arc::new(super::shared::ReplaySaltPool::new()),
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
    ) -> Result<(Session, super::stream::ShadowsocksAeadStream<S>), Error>
    where
        S: zero_traits::AsyncSocket,
    {
        let (accept, user) = self
            .profile
            .accept_request(&self.inbound, &mut stream)
            .await?;

        self.tcp_state.check_accept_replay(&accept)?;

        let mut session = accept.session.clone();
        session.apply_auth(user.auth());

        let client = self.profile.into_aead_stream(accept, &user, stream)?;

        Ok((session, client))
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundProfile {
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
        let cipher = super::shared::CipherKind::from_str(cipher_name)
            .ok_or(Error::Protocol("ss: unknown inbound cipher"))?;
        if identity_password.is_some()
            && !matches!(
                cipher,
                super::shared::CipherKind::Blake3Aes128Gcm
                    | super::shared::CipherKind::Blake3Aes256Gcm
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

    pub(crate) fn cipher(&self) -> super::shared::CipherKind {
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
            super::shared::identify_udp_2022_user(self.cipher, identity_password, datagram)?;
        self.users_snapshot()
            .find_identity(&identity)
            .map(|(_, user)| user.clone())
            .ok_or(Error::Protocol("ss: SIP023 udp user identity not found"))
    }

    pub fn is_2022(&self) -> bool {
        self.cipher.is_blake3()
    }

    pub fn tcp_state(&self) -> ShadowsocksInboundTcpState {
        ShadowsocksInboundTcpState::new(self.cipher)
    }

    pub fn udp_codec(&self) -> ShadowsocksInboundUdpCodec {
        let users = self.users_snapshot();
        let password = users
            .first()
            .map(ShadowsocksUser::password)
            .unwrap_or_default();
        match self.identity_password() {
            Some(identity_password) => {
                ShadowsocksInboundUdpCodec::new_eih(self.cipher, identity_password, password)
            }
            None => ShadowsocksInboundUdpCodec::new(self.cipher, password),
        }
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
    ) -> Result<super::stream::ShadowsocksAeadStream<S>, Error> {
        accept.into_aead_stream(inner, user.password())
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Default)]
pub struct ShadowsocksInboundProfileStore {
    profiles: Arc<Mutex<HashMap<String, ShadowsocksInboundProfile>>>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundProfileStore {
    pub fn replace(
        &self,
        tag: &str,
        cipher: &str,
        users: &[ShadowsocksInboundUserRef<'_>],
    ) -> Result<ShadowsocksInboundProfile, Error> {
        self.replace_with_identity(tag, cipher, None, users)
    }

    pub fn replace_with_identity(
        &self,
        tag: &str,
        cipher: &str,
        identity_password: Option<&str>,
        users: &[ShadowsocksInboundUserRef<'_>],
    ) -> Result<ShadowsocksInboundProfile, Error> {
        let mut profiles = self
            .profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            if profile.cipher_name() == cipher
                && profile.identity_password() == identity_password.map(str::as_bytes)
            {
                profile.replace_config_users(users.iter().copied())?;
                return Ok(profile.clone());
            }
        }

        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            cipher,
            identity_password,
            users.iter().copied(),
        )?;
        profiles.insert(tag.to_owned(), profile.clone());
        Ok(profile)
    }
}

#[cfg(feature = "crypto")]
pub fn inbound_profile_from_config_cipher_password(
    cipher_name: &str,
    password: &str,
) -> Result<ShadowsocksInboundProfile, Error> {
    ShadowsocksInboundProfile::from_config_cipher_password(cipher_name, password)
}

impl ShadowsocksInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("shadowsocks")
    }

    /// Decrypt the initial stream payload, extract target address,
    /// and return session key + remaining payload for relay.
    #[cfg(feature = "crypto")]
    pub async fn accept_request<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        if cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                return self.accept_request_2022(stream, cipher, password).await;
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: 2022 tcp accept requires `blake3` feature",
            ));
        }
        self.accept_request_legacy(stream, cipher, password).await
    }

    #[cfg(feature = "crypto")]
    async fn accept_request_users<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        identity_password: Option<&[u8]>,
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        if users.is_empty() {
            return Err(Error::Protocol("ss: no authorized inbound users"));
        }
        if cipher.is_blake3() {
            let Some(identity_password) = identity_password else {
                if users.len() > 1 {
                    return Err(Error::Protocol(
                        "ss: 2022 multi-user tcp accept requires a SIP023 identity key",
                    ));
                }
                return self
                    .accept_request(stream, cipher, users[0].password())
                    .await
                    .map(|accept| (accept, 0));
            };
            #[cfg(feature = "blake3")]
            {
                return self
                    .accept_request_2022_eih(stream, cipher, identity_password, users)
                    .await;
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: SIP023 tcp accept requires `blake3` feature",
            ));
        }
        if users.len() == 1 {
            return self
                .accept_request(stream, cipher, users[0].password())
                .await
                .map(|accept| (accept, 0));
        }
        self.accept_request_legacy_users(stream, cipher, users)
            .await
    }

    #[cfg(feature = "crypto")]
    async fn accept_request_legacy_users<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        use super::shared::{
            decrypt_tcp_chunk_length, decrypt_tcp_chunk_payload, derive_session_key,
            parse_target_data, read_exact, TCP_CHUNK_SIZE_LEN,
        };

        let salt_len = cipher.salt_len();
        let mut salt = vec![0u8; salt_len];
        read_exact(stream, &mut salt).await?;
        let mut encrypted_length = vec![0u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
        read_exact(stream, &mut encrypted_length).await?;

        let mut matched = None;
        for (index, user) in users.iter().enumerate() {
            let key = derive_session_key(cipher, user.password(), &salt)?;
            let mut nonce = 0;
            if let Ok(payload_len) =
                decrypt_tcp_chunk_length(cipher, &key, &mut nonce, &encrypted_length)
            {
                matched = Some((index, key, nonce, payload_len));
                break;
            }
        }
        let Some((user_index, key, mut nonce, payload_len)) = matched else {
            return Err(Error::Protocol("ss: inbound user authentication failed"));
        };

        let mut encrypted_payload = vec![0u8; payload_len + cipher.tag_len()];
        read_exact(stream, &mut encrypted_payload).await?;
        let plain =
            decrypt_tcp_chunk_payload(cipher, &key, &mut nonce, payload_len, &encrypted_payload)?;
        let (target, port, payload_offset) = parse_target_data(&plain)?;
        let session = Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        Ok((
            ShadowsocksAccept {
                session,
                remaining_payload: plain[payload_offset..].to_vec(),
                session_key: key,
                cipher,
                next_upload_nonce: nonce,
                request_salt: salt,
            },
            user_index,
        ))
    }

    #[cfg(all(feature = "crypto", feature = "blake3"))]
    async fn accept_request_2022_eih<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        identity_password: &[u8],
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        match self
            .accept_request_2022_eih_probe(stream, cipher, identity_password, users)
            .await
        {
            Ok(accept) => Ok(accept),
            Err(error) => {
                drain_stream(stream, SS_2022_DRAIN_CAP).await;
                Err(error)
            }
        }
    }

    #[cfg(all(feature = "crypto", feature = "blake3"))]
    async fn accept_request_2022_eih_probe<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        identity_password: &[u8],
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        use super::shared::{
            decrypt_tcp_2022_identity_header, decrypt_tcp_2022_single_chunk, derive_session_key,
            parse_2022_request_fixed_header, parse_2022_request_var_header,
            validate_2022_timestamp, SS_2022_HEADER_TYPE_CLIENT_STREAM,
            SS_2022_REQUEST_FIXED_HEADER_LEN,
        };

        let salt_len = cipher.salt_len();
        let fixed_size = SS_2022_REQUEST_FIXED_HEADER_LEN + cipher.tag_len();
        let mut head = vec![0u8; salt_len + 16 + fixed_size];
        let n = stream
            .read(&mut head)
            .await
            .map_err(|_| Error::Io("ss: 2022 request read failed"))?;
        if n < head.len() {
            return Err(Error::Protocol("ss: 2022 request header too short"));
        }

        let identity = decrypt_tcp_2022_identity_header(
            cipher,
            identity_password,
            &head[..salt_len],
            &head[salt_len..salt_len + 16],
        )?;
        let (user_index, user) = users
            .find_identity(&identity)
            .ok_or(Error::Protocol("ss: SIP023 tcp user identity not found"))?;
        let key = derive_session_key(cipher, user.password(), &head[..salt_len])?;
        let mut nonce = 0u64;
        let fixed_plain = decrypt_tcp_2022_single_chunk(
            cipher,
            &key,
            &mut nonce,
            &head[salt_len + 16..salt_len + 16 + fixed_size],
        )?;
        let (header_type, timestamp, var_len) = parse_2022_request_fixed_header(&fixed_plain)?;
        if header_type != SS_2022_HEADER_TYPE_CLIENT_STREAM {
            return Err(Error::Protocol("ss: SIP023 request header bad type"));
        }
        validate_2022_timestamp(timestamp)?;
        let var_len = var_len as usize;

        let var_size = var_len + cipher.tag_len();
        let mut enc_var = vec![0u8; var_size];
        let vn = stream
            .read(&mut enc_var)
            .await
            .map_err(|_| Error::Io("ss: 2022 variable header read failed"))?;
        if vn < var_size {
            return Err(Error::Protocol("ss: 2022 variable header too short"));
        }
        let var_plain =
            decrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &enc_var[..var_size])?;
        let (target, port, initial_payload) = parse_2022_request_var_header(&var_plain)?;
        let session = Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        Ok((
            ShadowsocksAccept {
                session,
                remaining_payload: initial_payload,
                session_key: key,
                cipher,
                next_upload_nonce: nonce,
                request_salt: head[..salt_len].to_vec(),
            },
            user_index,
        ))
    }

    /// Legacy AEAD accept: read salt + one length/payload chunk, extract target.
    #[cfg(feature = "crypto")]
    async fn accept_request_legacy<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        use super::shared::{derive_session_key, parse_target_data, read_exact, read_tcp_chunk};

        let salt_len = cipher.salt_len();

        // Read salt
        let mut salt = vec![0u8; salt_len];
        read_exact(stream, &mut salt).await?;

        let key = derive_session_key(cipher, password, &salt)?;

        let mut nonce = 0;
        let plain = read_tcp_chunk(stream, cipher, &key, &mut nonce).await?;

        // Parse target from plaintext
        let (target, port, payload_offset) = parse_target_data(&plain)?;
        let remaining_payload = plain[payload_offset..].to_vec();

        let session = Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );

        Ok(ShadowsocksAccept {
            session,
            remaining_payload,
            session_key: key,
            cipher,
            next_upload_nonce: nonce,
            request_salt: salt,
        })
    }

    /// 2022 edition (SIP022) accept: read salt + fixed-header chunk (nonce 0)
    /// + variable-header chunk (nonce 1). Body chunks follow from nonce 2.
    ///
    /// Implements SIP022 3.1.3 detection prevention: the salt + fixed-length
    /// header are read in a single `read()` call, and on any handshake failure
    /// the stream is drained before returning so the subsequent close sends FIN
    /// rather than RST (hiding how many bytes the server consumed).
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    async fn accept_request_2022<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        match self
            .accept_request_2022_probe(stream, cipher, password)
            .await
        {
            Ok(accept) => Ok(accept),
            Err(error) => {
                // Drain to hide byte consumption from active probers.
                drain_stream(stream, SS_2022_DRAIN_CAP).await;
                Err(error)
            }
        }
    }

    /// Single-read + validate the 2022 request, without drain-on-error. The
    /// caller ([`accept_request_2022`]) drains on failure.
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    async fn accept_request_2022_probe<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        use super::shared::{
            decrypt_tcp_2022_single_chunk, derive_session_key, parse_2022_request_fixed_header,
            parse_2022_request_var_header, validate_2022_timestamp,
            SS_2022_HEADER_TYPE_CLIENT_STREAM, SS_2022_REQUEST_FIXED_HEADER_LEN,
        };

        let salt_len = cipher.salt_len();
        let fixed_size = SS_2022_REQUEST_FIXED_HEADER_LEN + cipher.tag_len();

        // SIP022 3.1.3: exactly ONE read for salt + fixed-length header. A
        // short read means a probe (or a fragmenting path); reject it.
        let mut head = vec![0u8; salt_len + fixed_size];
        let n = stream
            .read(&mut head)
            .await
            .map_err(|_| Error::Io("ss: 2022 request read failed"))?;
        if n < salt_len + fixed_size {
            return Err(Error::Protocol("ss: 2022 request header too short"));
        }

        let key = derive_session_key(cipher, password, &head[..salt_len])?;
        let mut nonce = 0u64;
        let fixed_plain = decrypt_tcp_2022_single_chunk(
            cipher,
            &key,
            &mut nonce,
            &head[salt_len..salt_len + fixed_size],
        )?;
        let (header_type, timestamp, var_len) = parse_2022_request_fixed_header(&fixed_plain)?;
        if header_type != SS_2022_HEADER_TYPE_CLIENT_STREAM {
            return Err(Error::Protocol("ss: 2022 request header bad type"));
        }
        validate_2022_timestamp(timestamp)?;

        // Variable-length header: one read of its AEAD chunk.
        let var_len = var_len as usize;
        let var_size = var_len + cipher.tag_len();
        let mut enc_var = vec![0u8; var_size];
        let vn = stream
            .read(&mut enc_var)
            .await
            .map_err(|_| Error::Io("ss: 2022 variable header read failed"))?;
        if vn < var_size {
            return Err(Error::Protocol("ss: 2022 variable header too short"));
        }
        let var_plain =
            decrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &enc_var[..var_size])?;
        if var_plain.len() != var_len {
            return Err(Error::Protocol("ss: 2022 variable header length mismatch"));
        }
        let (target, port, initial_payload) = parse_2022_request_var_header(&var_plain)?;

        let session = Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );

        Ok(ShadowsocksAccept {
            session,
            remaining_payload: initial_payload,
            session_key: key,
            cipher,
            next_upload_nonce: nonce,
            request_salt: head[..salt_len].to_vec(),
        })
    }

    /// Encrypt a plaintext chunk for the server-to-client direction.
    #[cfg(feature = "crypto")]
    pub fn encrypt_chunk(
        cipher: super::shared::CipherKind,
        key: &[u8],
        nonce_counter: &mut u64,
        data: &[u8],
    ) -> Result<Vec<u8>, Error> {
        super::shared::encrypt_tcp_chunk(cipher, key, nonce_counter, data)
    }

    /// Decrypt a ciphertext chunk for the client-to-server direction.
    #[cfg(feature = "crypto")]
    pub fn decrypt_chunk(
        cipher: super::shared::CipherKind,
        key: &[u8],
        nonce_counter: &mut u64,
        data: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let length_size = super::shared::TCP_CHUNK_SIZE_LEN + cipher.tag_len();
        if data.len() < length_size {
            return Err(Error::Protocol("ss: chunk too short"));
        }
        let payload_len = super::shared::decrypt_tcp_chunk_length(
            cipher,
            key,
            nonce_counter,
            &data[..length_size],
        )?;
        super::shared::decrypt_tcp_chunk_payload(
            cipher,
            key,
            nonce_counter,
            payload_len,
            &data[length_size..],
        )
    }
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
/// SIP022 3.1.3 detection-prevention drain cap (bytes). Bounds the drain so a
/// malicious peer cannot hold the connection open indefinitely; typical active
/// probes send far fewer bytes than this.
const SS_2022_DRAIN_CAP: usize = 1 << 20; // 1 MiB
#[cfg(all(feature = "crypto", feature = "blake3"))]
/// Hard wall on how long a failed-handshake drain may block. A peer that sends
/// a short probe and then holds the connection open would otherwise pin a task
/// until the byte cap is reached; this keeps the anti-probe drain bounded.
const SS_2022_DRAIN_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(2);

/// Drain up to `cap` bytes (or `timeout`, or EOF) from `stream`, discarding
/// them. Used after a failed 2022 handshake so closing the connection sends FIN
/// (empty receive buffer) instead of RST, hiding how many bytes the server
/// consumed. Bounded by both a byte cap and a wall-clock timeout so a peer
/// cannot pin the task by keeping the connection open after a short probe.
#[cfg(all(feature = "crypto", feature = "blake3"))]
async fn drain_stream<S: zero_traits::AsyncSocket>(stream: &mut S, cap: usize) {
    let mut buf = [0u8; 4096];
    let mut total = 0usize;
    let mut deadline_reached = false;
    while total < cap && !deadline_reached {
        // Bound each read so a silent peer cannot block forever.
        match tokio::time::timeout(SS_2022_DRAIN_TIMEOUT, stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => total += n,
            Ok(Err(_)) => break,
            Err(_) => {
                deadline_reached = true;
            }
        }
    }
}
