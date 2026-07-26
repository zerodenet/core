// Hysteria2 inbound protocol — inbound.rs

use alloc::string::String;
use alloc::vec::Vec;
#[cfg(feature = "crypto")]
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use zero_core::{Error, InboundClientResponse, Network, ProtocolType, Session, SessionAuth};
use zero_traits::AsyncSocket;

/// Hysteria2 inbound handler — validates client auth and dispatches streams.
#[derive(Debug, Default, Clone, Copy)]
pub struct Hysteria2Inbound;

/// Per-user configuration for Hysteria2 authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2User {
    pub password: String,
    pub principal_key: Option<String>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct Hysteria2InboundUserRef<'a> {
    pub password: &'a str,
    pub principal_key: Option<&'a str>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

/// Protocol-owned validated inbound profile.
///
/// Proxy listener code owns QUIC accept and task scheduling; this profile owns
/// Hysteria2 authentication material and protocol response framing.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct Hysteria2InboundProfile {
    users: Arc<RwLock<Arc<[Hysteria2User]>>>,
}

/// Protocol-owned TCP stream accept/response helper.
///
/// Proxy code owns QUIC connection scheduling, while this type owns Hysteria2
/// TCP connect request parsing and connect response framing.
#[derive(Debug, Default, Clone, Copy)]
pub struct Hysteria2InboundTcpAcceptor {
    inbound: Hysteria2Inbound,
}

#[cfg(all(feature = "tokio", feature = "crypto"))]
pub struct Hysteria2AcceptedQuicConnection {
    conn: std::sync::Arc<quinn::Connection>,
    tcp_acceptor: Hysteria2InboundTcpAcceptor,
    auth: SessionAuth,
}

#[cfg(all(feature = "tokio", feature = "crypto"))]
impl Hysteria2AcceptedQuicConnection {
    pub fn new(conn: std::sync::Arc<quinn::Connection>, auth: SessionAuth) -> Self {
        Self {
            conn,
            tcp_acceptor: Hysteria2InboundTcpAcceptor::new(),
            auth,
        }
    }

    pub fn connection(&self) -> std::sync::Arc<quinn::Connection> {
        self.conn.clone()
    }

    pub fn auth(&self) -> &SessionAuth {
        &self.auth
    }

    pub fn close(&self, reason: &str) {
        self.conn
            .close(quinn::VarInt::from_u32(0), reason.as_bytes());
    }

    pub fn accept_udp_session(&self) -> crate::udp::Hysteria2InboundUdpRelay {
        crate::udp::Hysteria2InboundUdpRelay::with_auth(
            Hysteria2Inbound.udp_responder(),
            self.auth.clone(),
        )
    }

    pub async fn accept_next_tcp_stream<S, F>(
        &self,
        stream_factory: F,
    ) -> Result<Option<(Session, S)>, Error>
    where
        S: AsyncSocket,
        F: FnOnce(quinn::SendStream, quinn::RecvStream) -> S,
    {
        let (send, recv) = self
            .conn
            .accept_bi()
            .await
            .map_err(|_| Error::Io("hysteria2: accept tcp stream"))?;
        let mut stream = stream_factory(send, recv);
        let mut session = self.tcp_acceptor.accept_stream(&mut stream).await?;
        session.apply_auth(self.auth.clone());
        Ok(Some((session, stream)))
    }
}

impl Hysteria2InboundTcpAcceptor {
    pub fn new() -> Self {
        Self {
            inbound: Hysteria2Inbound,
        }
    }

    pub async fn accept_stream<S>(&self, stream: &mut S) -> Result<Session, Error>
    where
        S: AsyncSocket,
    {
        self.inbound.accept_tcp_stream(stream).await
    }

    pub async fn send_ok<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.inbound.send_connect_ok(stream).await
    }

    pub async fn send_error<S>(&self, stream: &mut S, message: &str) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.inbound.send_connect_error(stream, message).await
    }
}

impl<S> InboundClientResponse<S> for Hysteria2InboundTcpAcceptor
where
    S: AsyncSocket,
{
    async fn send_ok(&self, client: &mut S) -> Result<(), Error> {
        Hysteria2InboundTcpAcceptor::send_ok(self, client).await
    }

    async fn send_blocked(&self, client: &mut S) -> Result<(), Error> {
        let _ = Hysteria2InboundTcpAcceptor::send_error(self, client, "blocked").await;
        Ok(())
    }

    async fn send_upstream_failure(&self, client: &mut S) -> Result<(), Error> {
        let _ = Hysteria2InboundTcpAcceptor::send_error(self, client, "outbound failed").await;
        Ok(())
    }
}

#[cfg(feature = "crypto")]
impl Hysteria2InboundProfile {
    pub fn from_config(password: &str) -> Self {
        Self::from_config_users([Hysteria2InboundUserRef {
            password,
            principal_key: None,
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: None,
        }])
    }

    pub fn from_config_users<'a, I>(users: I) -> Self
    where
        I: IntoIterator<Item = Hysteria2InboundUserRef<'a>>,
    {
        Self {
            users: Arc::new(RwLock::new(
                users
                    .into_iter()
                    .map(Hysteria2User::from_ref)
                    .collect::<Vec<_>>()
                    .into(),
            )),
        }
    }

    pub fn from_config_parts(password: &str) -> Self {
        Self::from_config(password)
    }

    pub fn from_config_password(password: &str) -> Self {
        Self::from_config_parts(password)
    }

    pub fn user_count(&self) -> usize {
        self.users_snapshot().len()
    }

    pub fn replace_config_users<'a, I>(&self, users: I)
    where
        I: IntoIterator<Item = Hysteria2InboundUserRef<'a>>,
    {
        let users = users
            .into_iter()
            .map(Hysteria2User::from_ref)
            .collect::<Vec<_>>();
        *self
            .users
            .write()
            .unwrap_or_else(|error| error.into_inner()) = users.into();
    }

    fn users_snapshot(&self) -> Arc<[Hysteria2User]> {
        self.users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn authenticate_client(
        &self,
        salt: &[u8; 32],
        auth_frame: &[u8],
    ) -> Result<SessionAuth, Error> {
        let client_hmac = crate::shared::parse_auth_frame(auth_frame)?;
        self.authenticate_hmac(salt, &client_hmac)
    }

    pub fn authenticate_hmac(
        &self,
        salt: &[u8; 32],
        client_hmac: &[u8; 32],
    ) -> Result<SessionAuth, Error> {
        self.users_snapshot()
            .iter()
            .find(|user| crate::shared::verify_hmac(&user.password, salt, client_hmac))
            .map(Hysteria2User::auth)
            .ok_or(Error::Protocol("hysteria2: authentication failed"))
    }

    pub(crate) fn authenticate_password(&self, password: &str) -> Result<SessionAuth, Error> {
        self.users_snapshot()
            .iter()
            .find(|user| user.password == password)
            .map(Hysteria2User::auth)
            .ok_or(Error::Protocol("hysteria2: authentication failed"))
    }

    fn auth_ok_response(&self) -> Vec<u8> {
        crate::shared::build_auth_ok()
    }

    fn auth_error_response(&self, message: &str) -> Vec<u8> {
        crate::shared::build_auth_error(message)
    }

    async fn authenticate_connection<S>(
        &self,
        stream: &mut S,
        salt: &[u8; 32],
    ) -> Result<SessionAuth, Error>
    where
        S: AsyncSocket,
    {
        let mut auth_buf = [0u8; 64];
        let n = stream
            .read(&mut auth_buf)
            .await
            .map_err(|_| Error::Io("hysteria2: read auth"))?;
        if n == 0 {
            return Err(Error::Protocol("hysteria2: EOF on auth stream"));
        }

        let auth = match self.authenticate_client(salt, &auth_buf[..n]) {
            Ok(auth) => auth,
            Err(_) => {
                let err_resp = self.auth_error_response("authentication failed");
                let _ = stream.write_all(&err_resp).await;
                return Err(Error::Protocol("hysteria2: auth failed"));
            }
        };

        let ok_resp = self.auth_ok_response();
        stream
            .write_all(&ok_resp)
            .await
            .map_err(|_| Error::Io("hysteria2: write auth ok"))?;
        Ok(auth)
    }

    #[cfg(all(feature = "tokio", feature = "crypto"))]
    async fn authenticate_quic_connection<S>(
        &self,
        conn: &quinn::Connection,
        stream: &mut S,
    ) -> Result<SessionAuth, Error>
    where
        S: AsyncSocket,
    {
        let mut salt = [0u8; 32];
        conn.export_keying_material(&mut salt, b"hysteria2 auth", &[])
            .map_err(|_| Error::Io("hysteria2 key export failed"))?;

        self.authenticate_connection(stream, &salt).await
    }

    #[cfg(all(feature = "tokio", feature = "crypto"))]
    async fn accept_authenticated_quic_connection<S, F>(
        &self,
        conn: &quinn::Connection,
        stream_factory: F,
    ) -> Result<SessionAuth, Error>
    where
        S: AsyncSocket,
        F: FnOnce(quinn::SendStream, quinn::RecvStream) -> S,
    {
        let (send, recv) = conn
            .accept_bi()
            .await
            .map_err(|_| Error::Io("hysteria2: accept auth stream"))?;
        let mut auth_stream = stream_factory(send, recv);
        self.authenticate_quic_connection(conn, &mut auth_stream)
            .await
    }

    #[cfg(all(feature = "tokio", feature = "crypto"))]
    pub async fn accept_authenticated_quic_session<S, F>(
        &self,
        conn: quinn::Connection,
        stream_factory: F,
    ) -> Result<Hysteria2AcceptedQuicConnection, Error>
    where
        S: AsyncSocket,
        F: FnOnce(quinn::SendStream, quinn::RecvStream) -> S,
    {
        let auth = self
            .accept_authenticated_quic_connection(&conn, stream_factory)
            .await?;
        let conn = std::sync::Arc::new(conn);
        Ok(Hysteria2AcceptedQuicConnection::new(conn, auth))
    }
}

#[cfg(feature = "crypto")]
impl core::fmt::Debug for Hysteria2InboundProfile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Hysteria2InboundProfile")
            .field("user_count", &self.user_count())
            .finish()
    }
}

impl Hysteria2User {
    fn from_ref(user: Hysteria2InboundUserRef<'_>) -> Self {
        Self {
            password: user.password.to_owned(),
            principal_key: user.principal_key.map(str::to_owned),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        }
    }

    fn auth(&self) -> SessionAuth {
        let mut auth = SessionAuth::new("hysteria2");
        auth.principal_key = self
            .principal_key
            .clone()
            .or_else(|| Some(self.password.clone()));
        auth.up_bps = self.up_bps;
        auth.down_bps = self.down_bps;
        auth.device_limit = self.device_limit;
        auth.quota_remaining_bytes = self.quota_remaining_bytes;
        auth.policy_revision = self.policy_revision;
        auth
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Default)]
pub struct Hysteria2InboundProfileStore {
    profiles: Arc<Mutex<HashMap<String, Hysteria2InboundProfile>>>,
}

#[cfg(feature = "crypto")]
impl Hysteria2InboundProfileStore {
    pub fn replace(
        &self,
        tag: &str,
        users: &[Hysteria2InboundUserRef<'_>],
    ) -> Hysteria2InboundProfile {
        let mut profiles = self
            .profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            profile.replace_config_users(users.iter().copied());
            return profile.clone();
        }
        let profile = Hysteria2InboundProfile::from_config_users(users.iter().copied());
        profiles.insert(tag.to_owned(), profile.clone());
        profile
    }
}

#[cfg(feature = "crypto")]
pub fn inbound_profile_from_config_password(password: &str) -> Hysteria2InboundProfile {
    Hysteria2InboundProfile::from_config_password(password)
}

/// Trait for looking up Hysteria2 users by password validation.
pub trait Hysteria2UserStore {
    fn validate_password(&self, hmac: &[u8; 32], salt: &[u8; 32]) -> Option<&Hysteria2User>;
}

impl Hysteria2Inbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("hysteria2")
    }

    #[cfg(feature = "tokio")]
    pub fn udp_session(&self) -> crate::udp::Hysteria2InboundUdpSession {
        crate::udp::Hysteria2InboundUdpSession::new()
    }

    #[cfg(feature = "tokio")]
    pub fn udp_responder(&self) -> crate::udp::Hysteria2InboundUdpResponder {
        crate::udp::Hysteria2InboundUdpResponder::new(self.udp_session())
    }

    #[cfg(feature = "tokio")]
    pub fn accept_udp_session(&self) -> crate::udp::Hysteria2InboundUdpRelay {
        crate::udp::Hysteria2InboundUdpRelay::new(self.udp_responder())
    }

    pub fn accept_tcp_connect_header(&self, header: &[u8]) -> Result<Session, Error> {
        let (target, port) = crate::shared::parse_tcp_connect_header(header)?;
        Ok(Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("hysteria2"),
        ))
    }

    pub async fn accept_tcp_stream<S>(&self, stream: &mut S) -> Result<Session, Error>
    where
        S: AsyncSocket,
    {
        let mut header_buf = [0u8; 512];
        let n = stream
            .read(&mut header_buf)
            .await
            .map_err(|_| Error::Io("hysteria2: read tcp connect header"))?;
        if n == 0 {
            return Err(Error::Protocol("hysteria2: EOF on tcp connect stream"));
        }
        self.accept_tcp_connect_header(&header_buf[..n])
    }

    pub fn connect_ok_response(&self) -> Vec<u8> {
        crate::shared::build_connect_ok()
    }

    pub fn connect_error_response(&self, message: &str) -> Vec<u8> {
        crate::shared::build_connect_error(message)
    }

    pub async fn send_connect_ok<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let response = self.connect_ok_response();
        stream
            .write_all(&response)
            .await
            .map_err(|_| Error::Io("hysteria2: write connect ok"))
    }

    pub async fn send_connect_error<S>(&self, stream: &mut S, message: &str) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let response = self.connect_error_response(message);
        stream
            .write_all(&response)
            .await
            .map_err(|_| Error::Io("hysteria2: write connect error"))
    }

    /// Validate client authentication using HMAC-SHA256(password, salt).
    pub fn validate_auth(
        &self,
        hmac: &[u8; 32],
        salt: &[u8; 32],
        store: &impl Hysteria2UserStore,
    ) -> Result<Session, Error> {
        store
            .validate_password(hmac, salt)
            .ok_or(Error::Protocol("hysteria2: authentication failed"))?;

        let auth = SessionAuth::new("hysteria2");
        let mut session = Session::new(
            0,
            zero_core::Address::Domain(String::new()),
            0,
            zero_core::Network::Tcp,
            ProtocolType::new("hysteria2"),
        );
        session.auth = Some(auth);
        Ok(session)
    }
}
