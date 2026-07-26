//! Trojan inbound protocol handler.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use zero_core::{Error, InboundStreamUdpRelay, Network, ProtocolType, Session, SessionAuth};
use zero_traits::AsyncSocket;

use super::shared::{read_password, read_request, CMD_TCP, CMD_UDP};
use crate::udp::TrojanInboundUdpResponder;

/// Trojan inbound handler.
#[derive(Debug, Default, Clone, Copy)]
pub struct TrojanInbound;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrojanUser {
    pub password: String,
    pub principal_key: Option<String>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

impl TrojanUser {
    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        password: impl Into<String>,
        principal_key: Option<String>,
        up_bps: Option<u64>,
        down_bps: Option<u64>,
        device_limit: Option<u32>,
        quota_remaining_bytes: Option<u64>,
        policy_revision: Option<u64>,
    ) -> Self {
        Self {
            password: password.into(),
            principal_key,
            up_bps,
            down_bps,
            device_limit,
            quota_remaining_bytes,
            policy_revision,
        }
    }

    fn auth(&self) -> SessionAuth {
        SessionAuth {
            scheme: "trojan".into(),
            principal_key: self
                .principal_key
                .clone()
                .or_else(|| Some(self.password.clone())),
            up_bps: self.up_bps,
            down_bps: self.down_bps,
            device_limit: self.device_limit,
            quota_remaining_bytes: self.quota_remaining_bytes,
            policy_revision: self.policy_revision,
        }
    }
}

pub type TrojanInboundUserConfigParts = (
    String,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
    Option<u64>,
);

#[derive(Debug, Clone, Copy)]
pub struct TrojanInboundUserRef<'a> {
    pub password: &'a str,
    pub principal_key: Option<&'a str>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

#[derive(Clone)]
pub struct TrojanInboundProfile {
    users: Arc<RwLock<Arc<[TrojanUser]>>>,
}

impl core::fmt::Debug for TrojanInboundProfile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TrojanInboundProfile")
            .field("user_count", &self.user_count())
            .finish()
    }
}

impl TrojanInboundProfile {
    pub fn from_config(password: impl Into<String>) -> Self {
        Self::from_users(vec![TrojanUser::from_config(
            password, None, None, None, None, None, None,
        )])
    }

    pub fn from_config_parts(password: impl Into<String>) -> Self {
        Self::from_config(password)
    }

    pub fn from_config_password(password: impl Into<String>) -> Self {
        Self::from_config_parts(password)
    }

    pub fn from_users(users: Vec<TrojanUser>) -> Self {
        Self {
            users: Arc::new(RwLock::new(users.into())),
        }
    }

    pub fn user_count(&self) -> usize {
        self.users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    pub fn replace_users(&self, users: Vec<TrojanUser>) {
        *self
            .users
            .write()
            .unwrap_or_else(|error| error.into_inner()) = users.into();
    }

    pub fn from_config_users<I, U>(users: I) -> Self
    where
        I: IntoIterator<Item = U>,
        U: IntoTrojanInboundUserConfig,
    {
        Self::from_users(
            users
                .into_iter()
                .map(IntoTrojanInboundUserConfig::into_trojan_inbound_user_config)
                .map(
                    |(
                        password,
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )| {
                        TrojanUser::from_config(
                            password,
                            principal_key,
                            up_bps,
                            down_bps,
                            device_limit,
                            quota_remaining_bytes,
                            policy_revision,
                        )
                    },
                )
                .collect(),
        )
    }

    pub fn replace_config_users<I, U>(&self, users: I)
    where
        I: IntoIterator<Item = U>,
        U: IntoTrojanInboundUserConfig,
    {
        let replacement = users
            .into_iter()
            .map(IntoTrojanInboundUserConfig::into_trojan_inbound_user_config)
            .map(
                |(
                    password,
                    principal_key,
                    up_bps,
                    down_bps,
                    device_limit,
                    quota_remaining_bytes,
                    policy_revision,
                )| {
                    TrojanUser::from_config(
                        password,
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )
                },
            )
            .collect();
        self.replace_users(replacement);
    }

    async fn accept<S: AsyncSocket>(
        &self,
        inbound: TrojanInbound,
        stream: &mut S,
    ) -> Result<(TrojanAccept, SessionAuth), Error> {
        let users = self
            .users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let accept = inbound.accept(stream, &users).await?;
        let auth = users
            .get(accept.user_index)
            .expect("accepted Trojan user index must exist")
            .auth();
        Ok((accept, auth))
    }

    pub async fn accept_session<S: AsyncSocket>(
        &self,
        inbound: TrojanInbound,
        stream: &mut S,
    ) -> Result<Session, Error> {
        let (accept, auth) = self.accept(inbound, stream).await?;
        let mut session = accept.session;
        session.apply_auth(auth);
        Ok(session)
    }

    pub async fn accept_client<S: AsyncSocket>(
        &self,
        inbound: TrojanInbound,
        mut stream: S,
    ) -> Result<TrojanInboundAcceptedSession<S>, Error> {
        let session = self.accept_session(inbound, &mut stream).await?;
        Ok(TrojanInboundAcceptedSession::from_session_stream(
            session, stream,
        ))
    }

    pub async fn accept_client_owned<S: AsyncSocket>(
        self,
        inbound: TrojanInbound,
        mut stream: S,
    ) -> Result<TrojanInboundAcceptedSession<S>, Error> {
        let session = self.accept_session(inbound, &mut stream).await?;
        Ok(TrojanInboundAcceptedSession::from_session_stream(
            session, stream,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TrojanInboundProfileStore {
    profiles: Arc<Mutex<HashMap<String, TrojanInboundProfile>>>,
}

impl TrojanInboundProfileStore {
    pub fn replace(&self, tag: &str, users: &[TrojanInboundUserRef<'_>]) -> TrojanInboundProfile {
        let mut profiles = self
            .profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            profile.replace_config_users(users.iter().copied());
            return profile.clone();
        }

        let profile = TrojanInboundProfile::from_config_users(users.iter().copied());
        profiles.insert(tag.to_owned(), profile.clone());
        profile
    }
}

pub trait IntoTrojanInboundUserConfig {
    fn into_trojan_inbound_user_config(self) -> TrojanInboundUserConfigParts;
}

impl IntoTrojanInboundUserConfig for TrojanInboundUserConfigParts {
    fn into_trojan_inbound_user_config(self) -> TrojanInboundUserConfigParts {
        self
    }
}

impl IntoTrojanInboundUserConfig for TrojanInboundUserRef<'_> {
    fn into_trojan_inbound_user_config(self) -> TrojanInboundUserConfigParts {
        (
            self.password.to_owned(),
            self.principal_key.map(str::to_owned),
            self.up_bps,
            self.down_bps,
            self.device_limit,
            self.quota_remaining_bytes,
            self.policy_revision,
        )
    }
}

/// Result of accepting a Trojan connection.
struct TrojanAccept {
    session: Session,
    user_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrojanInboundSessionKind {
    Tcp,
    Udp,
}

enum TrojanInboundAcceptedSessionState<S> {
    Tcp {
        session: Session,
        stream: S,
    },
    Udp {
        session: Session,
        relay: TrojanInboundUdpRelay<S>,
    },
}

pub struct TrojanInboundAcceptedSession<S> {
    state: TrojanInboundAcceptedSessionState<S>,
}

pub struct TrojanInboundUdpRelay<S> {
    auth: Option<SessionAuth>,
    responder: TrojanInboundUdpResponder,
    stream: S,
}

fn classify_inbound_session(session: &Session) -> TrojanInboundSessionKind {
    match session.network {
        Network::Udp => TrojanInboundSessionKind::Udp,
        Network::Tcp => TrojanInboundSessionKind::Tcp,
    }
}

impl<S> TrojanInboundUdpRelay<S> {
    fn new(stream: S, responder: TrojanInboundUdpResponder, auth: Option<SessionAuth>) -> Self {
        Self {
            auth,
            responder,
            stream,
        }
    }

    fn into_parts(self) -> (S, TrojanInboundUdpResponder, Option<SessionAuth>) {
        (self.stream, self.responder, self.auth)
    }
}

impl<S> InboundStreamUdpRelay for TrojanInboundUdpRelay<S>
where
    S: AsyncSocket,
{
    type Stream = S;
    type Responder = TrojanInboundUdpResponder;

    fn into_stream_udp_parts(self) -> (Self::Stream, Self::Responder, Option<SessionAuth>) {
        self.into_parts()
    }
}

impl<S> TrojanInboundAcceptedSession<S> {
    fn tcp(session: Session, stream: S) -> Self {
        Self {
            state: TrojanInboundAcceptedSessionState::Tcp { session, stream },
        }
    }

    fn udp(session: Session, relay: TrojanInboundUdpRelay<S>) -> Self {
        Self {
            state: TrojanInboundAcceptedSessionState::Udp { session, relay },
        }
    }

    fn from_session_stream(session: Session, stream: S) -> Self {
        match classify_inbound_session(&session) {
            TrojanInboundSessionKind::Tcp => Self::tcp(session, stream),
            TrojanInboundSessionKind::Udp => {
                let auth = session.auth.clone();
                Self::udp(
                    session,
                    TrojanInboundUdpRelay::new(stream, TrojanInbound.accept_udp_session(), auth),
                )
            }
        }
    }

    async fn dispatch<Tcp, TcpFut, Udp, UdpFut, E>(self, tcp: Tcp, udp: Udp) -> Result<(), E>
    where
        Tcp: FnOnce(Session, S) -> TcpFut,
        TcpFut: core::future::Future<Output = Result<(), E>>,
        Udp: FnOnce(Session, TrojanInboundUdpRelay<S>) -> UdpFut,
        UdpFut: core::future::Future<Output = Result<(), E>>,
    {
        match self.state {
            TrojanInboundAcceptedSessionState::Tcp { session, stream } => {
                tcp(session, stream).await
            }
            TrojanInboundAcceptedSessionState::Udp { session, relay } => udp(session, relay).await,
        }
    }
}

#[async_trait::async_trait]
impl<S> zero_core::InboundStreamRoute for TrojanInboundAcceptedSession<S>
where
    S: AsyncSocket,
{
    type TcpStream = S;
    type UdpRelay = TrojanInboundUdpRelay<S>;

    async fn dispatch_inbound_route<E, FTcp, FTcpFut, FUdp, FUdpFut>(
        self,
        on_tcp: FTcp,
        on_udp: FUdp,
    ) -> Result<(), E>
    where
        FTcp: FnOnce(Session, Self::TcpStream) -> FTcpFut + Send,
        FTcpFut: core::future::Future<Output = Result<(), E>> + Send,
        FUdp: FnOnce(Session, Self::UdpRelay) -> FUdpFut + Send,
        FUdpFut: core::future::Future<Output = Result<(), E>> + Send,
    {
        self.dispatch(on_tcp, on_udp).await
    }
}

impl TrojanInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("trojan")
    }

    pub fn inbound_auth(&self, password: impl Into<String>) -> SessionAuth {
        let mut auth = SessionAuth::new("trojan");
        auth.principal_key = Some(password.into());
        auth
    }

    /// Accept a Trojan TCP connection.
    ///
    /// Reads password hash + command + target address from the stream.
    /// The password is validated against `passwords` (hex SHA224 hashes).
    async fn accept<S: AsyncSocket>(
        &self,
        stream: &mut S,
        users: &[TrojanUser],
    ) -> Result<TrojanAccept, Error> {
        let password_hash = read_password(stream).await?;

        // Validate password.
        let Some(user_index) = users.iter().position(|user| {
            #[cfg(feature = "crypto")]
            {
                use sha2::{Digest, Sha224};
                password_hash
                    == super::shared::hex::encode(&Sha224::digest(user.password.as_bytes()))
            }
            #[cfg(not(feature = "crypto"))]
            {
                let _ = (user, &password_hash);
                false
            }
        }) else {
            return Err(Error::Protocol("trojan: invalid password"));
        };

        let (cmd, addr, port) = read_request(stream).await?;

        let network = match cmd {
            CMD_TCP => Network::Tcp,
            CMD_UDP => Network::Udp,
            _ => return Err(Error::Protocol("trojan: unsupported command")),
        };

        Ok(TrojanAccept {
            session: Session::new(0, addr, port, network, ProtocolType::new("trojan")),
            user_index,
        })
    }
}
