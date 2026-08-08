use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

#[cfg(feature = "reality")]
use zero_core::InboundStreamUdpRelay;
use zero_core::{
    Address, Error, InboundFallbackCapture, Network, ProtocolType, Session, SessionAuth,
};
use zero_traits::AsyncSocket;

#[cfg(feature = "reality")]
use crate::flow::{decode_addons, flow_read_request, FLOW_ZERO_AEAD_V1};
#[cfg(not(feature = "reality"))]
use crate::shared::read_addon;
use crate::shared::{
    parse_uuid, read_address, read_exact, CMD_MUX, CMD_TCP, CMD_UDP, VLESS_VERSION,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct VlessInbound;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessUser {
    pub principal_key: Option<String>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
    pub flow: Option<&'static str>,
}

impl VlessUser {
    pub fn new() -> Self {
        Self {
            principal_key: None,
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: None,
            flow: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        flow: Option<&str>,
        principal_key: Option<String>,
        up_bps: Option<u64>,
        down_bps: Option<u64>,
        device_limit: Option<u32>,
        quota_remaining_bytes: Option<u64>,
        policy_revision: Option<u64>,
    ) -> Result<Self, Error> {
        #[cfg(feature = "reality")]
        let flow = flow.map(crate::flow::parse_inbound_flow).transpose()?;
        #[cfg(not(feature = "reality"))]
        let flow = {
            if flow.is_some() {
                return Err(Error::Unsupported(
                    "VLESS flow requires the `reality` feature",
                ));
            }
            None
        };
        Ok(Self {
            principal_key,
            up_bps,
            down_bps,
            device_limit,
            quota_remaining_bytes,
            policy_revision,
            flow,
        })
    }
}

impl Default for VlessUser {
    fn default() -> Self {
        Self::new()
    }
}

pub trait VlessUserStore {
    fn find_user(&self, id: &[u8; 16]) -> Option<VlessUser>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessConfiguredUser {
    pub id: [u8; 16],
    pub user: VlessUser,
}

pub type VlessInboundUserConfigParts = (
    String,
    Option<String>,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
    Option<u64>,
);

impl VlessConfiguredUser {
    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        id: &str,
        flow: Option<&str>,
        principal_key: Option<String>,
        up_bps: Option<u64>,
        down_bps: Option<u64>,
        device_limit: Option<u32>,
        quota_remaining_bytes: Option<u64>,
        policy_revision: Option<u64>,
    ) -> Result<Self, Error> {
        Ok(Self {
            id: parse_uuid(id)?,
            user: VlessUser::from_config(
                flow,
                principal_key,
                up_bps,
                down_bps,
                device_limit,
                quota_remaining_bytes,
                policy_revision,
            )?,
        })
    }
}

pub struct VlessConfiguredUsers<'a> {
    users: &'a [VlessConfiguredUser],
}

impl<'a> VlessConfiguredUsers<'a> {
    pub fn new(users: &'a [VlessConfiguredUser]) -> Self {
        Self { users }
    }
}

impl VlessUserStore for VlessConfiguredUsers<'_> {
    fn find_user(&self, id: &[u8; 16]) -> Option<VlessUser> {
        self.users
            .iter()
            .find(|user| &user.id == id)
            .map(|user| user.user.clone())
    }
}

struct VlessAcceptedSession {
    session: Session,
    mux_master_uuid: [u8; 16],
}

pub(crate) struct VlessAcceptedClient<S> {
    accepted: VlessAcceptedSession,
    stream: S,
}

enum VlessAcceptedClientRouteState<S> {
    Tcp {
        session: Session,
        stream: S,
    },
    #[cfg(feature = "reality")]
    Udp {
        session: Session,
        relay: VlessInboundUdpRelay<S>,
    },
    #[cfg(feature = "reality")]
    Mux {
        mux_server: crate::mux::VlessInboundMuxServer,
        stream: S,
    },
}

pub struct VlessAcceptedClientRoute<S> {
    state: VlessAcceptedClientRouteState<S>,
}

#[cfg(feature = "reality")]
pub struct VlessInboundUdpRelay<S> {
    auth: Option<SessionAuth>,
    responder: crate::udp::VlessInboundUdpResponder,
    stream: S,
}

pub(crate) struct VlessClientAcceptError<S> {
    error: Error,
    stream: S,
}

pub struct VlessFallbackReplay<S> {
    stream: S,
    replay_head: Vec<u8>,
}

enum VlessFallbackAlpnDecisionState<S> {
    Replay(VlessFallbackReplay<S>),
    Continue { stream: S, replay_head: Vec<u8> },
}

pub struct VlessFallbackAlpnDecision<S> {
    state: VlessFallbackAlpnDecisionState<S>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessFallbackAlpnPolicy {
    expected: Option<String>,
}

impl VlessFallbackAlpnPolicy {
    pub fn from_expected(expected: Option<&str>) -> Self {
        Self {
            expected: expected.map(String::from),
        }
    }

    pub fn matches_client_alpns<'a, I>(&self, client_alpns: I) -> bool
    where
        I: IntoIterator<Item = &'a str>,
    {
        let Some(expected) = self.expected.as_deref() else {
            return false;
        };
        client_alpns.into_iter().any(|alpn| alpn == expected)
    }
}

pub fn fallback_alpn_matches<'a, I>(expected: Option<&str>, client_alpns: I) -> bool
where
    I: IntoIterator<Item = &'a str>,
{
    VlessFallbackAlpnPolicy::from_expected(expected).matches_client_alpns(client_alpns)
}

pub fn fallback_replay_for_alpns<'a, I, S>(
    expected: Option<&str>,
    client_alpns: I,
    stream: S,
    replay_head: Vec<u8>,
) -> VlessFallbackAlpnDecision<S>
where
    I: IntoIterator<Item = &'a str>,
{
    if fallback_alpn_matches(expected, client_alpns) {
        VlessFallbackAlpnDecision::replay(VlessFallbackReplay::new(stream, replay_head))
    } else {
        VlessFallbackAlpnDecision::continue_with(stream, replay_head)
    }
}

impl VlessAcceptedSession {
    fn new(session: Session, user_id: [u8; 16]) -> Self {
        Self {
            session,
            mux_master_uuid: user_id,
        }
    }

    fn into_parts(self) -> (Session, [u8; 16]) {
        (self.session, self.mux_master_uuid)
    }
}

impl<S> VlessAcceptedClient<S> {
    fn new(accepted: VlessAcceptedSession, stream: S) -> Self {
        Self { accepted, stream }
    }

    fn into_parts(self) -> (Session, [u8; 16], S) {
        let (session, mux_master_uuid) = self.accepted.into_parts();
        (session, mux_master_uuid, self.stream)
    }

    pub(crate) async fn into_route_with_sni(
        self,
        sni: Option<String>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<VlessAcceptedClientRoute<S>, Error>
    where
        S: AsyncSocket,
    {
        let (mut session, mux_master_uuid, mut stream) = self.into_parts();
        match classify_inbound_session(&session) {
            VlessInboundSessionKind::Tcp => {
                session.sni = sni;
                Ok(VlessAcceptedClientRoute::tcp(session, stream))
            }
            VlessInboundSessionKind::Udp => {
                session.sni = sni;
                #[cfg(feature = "reality")]
                {
                    let auth = session.auth.clone();
                    let responder = VlessInbound
                        .accept_udp_session(&mut stream, &session)
                        .await?;
                    Ok(VlessAcceptedClientRoute::udp(
                        session,
                        VlessInboundUdpRelay::new(stream, responder, auth),
                    ))
                }
                #[cfg(not(feature = "reality"))]
                {
                    Err(Error::Unsupported(
                        "VLESS UDP requires the `reality` feature",
                    ))
                }
            }
            VlessInboundSessionKind::Mux => {
                #[cfg(feature = "reality")]
                {
                    let auth = session.auth.clone();
                    let mux_server = VlessInbound
                        .accept_mux_session_with_auth(
                            &mut stream,
                            mux_master_uuid,
                            auth,
                            mux_response_backlog,
                        )
                        .await?;
                    Ok(VlessAcceptedClientRoute::mux(mux_server, stream))
                }
                #[cfg(not(feature = "reality"))]
                {
                    let _ = mux_master_uuid;
                    Err(Error::Unsupported(
                        "VLESS MUX requires the `reality` feature",
                    ))
                }
            }
        }
    }
}

#[cfg(feature = "reality")]
impl<S> VlessInboundUdpRelay<S> {
    fn new(
        stream: S,
        responder: crate::udp::VlessInboundUdpResponder,
        auth: Option<SessionAuth>,
    ) -> Self {
        Self {
            auth,
            responder,
            stream,
        }
    }

    fn into_parts(self) -> (S, crate::udp::VlessInboundUdpResponder, Option<SessionAuth>) {
        (self.stream, self.responder, self.auth)
    }
}

#[cfg(feature = "reality")]
impl<S> InboundStreamUdpRelay for VlessInboundUdpRelay<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    type Stream = S;
    type Responder = crate::udp::VlessInboundUdpResponder;

    fn into_stream_udp_parts(self) -> (Self::Stream, Self::Responder, Option<SessionAuth>) {
        self.into_parts()
    }
}

impl<S> VlessAcceptedClientRoute<S> {
    fn tcp(session: Session, stream: S) -> Self {
        Self {
            state: VlessAcceptedClientRouteState::Tcp { session, stream },
        }
    }

    #[cfg(feature = "reality")]
    fn udp(session: Session, relay: VlessInboundUdpRelay<S>) -> Self {
        Self {
            state: VlessAcceptedClientRouteState::Udp { session, relay },
        }
    }

    #[cfg(feature = "reality")]
    fn mux(mux_server: crate::mux::VlessInboundMuxServer, stream: S) -> Self {
        Self {
            state: VlessAcceptedClientRouteState::Mux { mux_server, stream },
        }
    }

    #[cfg(feature = "reality")]
    async fn dispatch<Tcp, TcpFut, Udp, UdpFut, Mux, MuxFut, E>(
        self,
        tcp: Tcp,
        udp: Udp,
        mux: Mux,
    ) -> Result<(), E>
    where
        Tcp: FnOnce(Session, S) -> TcpFut,
        TcpFut: core::future::Future<Output = Result<(), E>>,
        Udp: FnOnce(Session, VlessInboundUdpRelay<S>) -> UdpFut,
        UdpFut: core::future::Future<Output = Result<(), E>>,
        Mux: FnOnce(crate::mux::VlessInboundMuxServer, S) -> MuxFut,
        MuxFut: core::future::Future<Output = Result<(), E>>,
    {
        match self.state {
            VlessAcceptedClientRouteState::Tcp { session, stream } => tcp(session, stream).await,
            VlessAcceptedClientRouteState::Udp { session, relay } => udp(session, relay).await,
            VlessAcceptedClientRouteState::Mux { mux_server, stream } => {
                mux(mux_server, stream).await
            }
        }
    }

    #[cfg(not(feature = "reality"))]
    async fn dispatch<Tcp, TcpFut, Udp, UdpFut, Mux, MuxFut, E>(
        self,
        tcp: Tcp,
        _udp: Udp,
        _mux: Mux,
    ) -> Result<(), E>
    where
        Tcp: FnOnce(Session, S) -> TcpFut,
        TcpFut: core::future::Future<Output = Result<(), E>>,
    {
        match self.state {
            VlessAcceptedClientRouteState::Tcp { session, stream } => tcp(session, stream).await,
        }
    }
}

#[cfg(feature = "reality")]
#[async_trait::async_trait]
impl<S> zero_core::InboundMuxStreamRoute for VlessAcceptedClientRoute<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    type TcpStream = S;
    type UdpRelay = VlessInboundUdpRelay<S>;
    type MuxReader = S;
    type MuxServer = crate::mux::VlessInboundMuxServer;

    async fn dispatch_inbound_route<E, FTcp, FTcpFut, FUdp, FUdpFut, FMux, FMuxFut>(
        self,
        on_tcp: FTcp,
        on_udp: FUdp,
        on_mux: FMux,
    ) -> Result<(), E>
    where
        FTcp: FnOnce(Session, Self::TcpStream) -> FTcpFut + Send,
        FTcpFut: core::future::Future<Output = Result<(), E>> + Send,
        FUdp: FnOnce(Session, Self::UdpRelay) -> FUdpFut + Send,
        FUdpFut: core::future::Future<Output = Result<(), E>> + Send,
        FMux: FnOnce(Self::MuxReader, Self::MuxServer) -> FMuxFut + Send,
        FMuxFut: core::future::Future<Output = Result<(), E>> + Send,
    {
        self.dispatch(on_tcp, on_udp, |mux_server, stream| {
            on_mux(stream, mux_server)
        })
        .await
    }
}

impl<S> VlessClientAcceptError<S> {
    fn new(error: Error, stream: S) -> Self {
        Self { error, stream }
    }

    pub(crate) fn into_fallback_replay(self) -> (Error, VlessFallbackReplay<S::Stream>)
    where
        S: InboundFallbackCapture,
    {
        let (stream, replay_head) = self.stream.into_fallback_replay_parts();
        (self.error, VlessFallbackReplay::new(stream, replay_head))
    }
}

impl<S> VlessFallbackReplay<S> {
    fn new(stream: S, replay_head: Vec<u8>) -> Self {
        Self {
            stream,
            replay_head,
        }
    }

    pub fn into_stream(self) -> S {
        self.stream
    }

    pub fn replay_head(&self) -> &[u8] {
        &self.replay_head
    }

    pub async fn write_replay_head<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: AsyncSocket,
    {
        if !self.replay_head.is_empty() {
            writer.write_all(&self.replay_head).await?;
        }
        Ok(())
    }

    pub async fn replay_to_upstream<W>(self, writer: &mut W) -> Result<S, W::Error>
    where
        W: AsyncSocket,
    {
        let Self {
            stream,
            replay_head,
        } = self;
        if !replay_head.is_empty() {
            writer.write_all(&replay_head).await?;
        }
        Ok(stream)
    }
}

impl<S> zero_core::InboundFallbackReplay for VlessFallbackReplay<S>
where
    S: Send,
{
    type Stream = S;

    async fn replay_to<'a, W>(self, upstream: &'a mut W) -> Result<Self::Stream, W::Error>
    where
        Self: 'a,
        W: AsyncSocket + Send + 'a,
    {
        self.replay_to_upstream(upstream).await
    }
}

impl<S> VlessFallbackAlpnDecision<S> {
    fn replay(replay: VlessFallbackReplay<S>) -> Self {
        Self {
            state: VlessFallbackAlpnDecisionState::Replay(replay),
        }
    }

    fn continue_with(stream: S, replay_head: Vec<u8>) -> Self {
        Self {
            state: VlessFallbackAlpnDecisionState::Continue {
                stream,
                replay_head,
            },
        }
    }

    pub fn into_transport_parts(self) -> Result<VlessFallbackReplay<S>, (S, Vec<u8>)> {
        match self.state {
            VlessFallbackAlpnDecisionState::Replay(replay) => Ok(replay),
            VlessFallbackAlpnDecisionState::Continue {
                stream,
                replay_head,
            } => Err((stream, replay_head)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VlessInboundSessionKind {
    Tcp,
    Udp,
    Mux,
}

fn classify_inbound_session(session: &Session) -> VlessInboundSessionKind {
    if VlessInbound::is_mux_session(session) {
        VlessInboundSessionKind::Mux
    } else {
        match session.network {
            Network::Udp => VlessInboundSessionKind::Udp,
            Network::Tcp => VlessInboundSessionKind::Tcp,
        }
    }
}

#[derive(Clone)]
pub struct VlessInboundProfile {
    users: Arc<std::sync::RwLock<Arc<[VlessConfiguredUser]>>>,
}

impl core::fmt::Debug for VlessInboundProfile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("VlessInboundProfile")
            .field("user_count", &self.user_count())
            .finish()
    }
}

impl VlessInboundProfile {
    pub fn from_users(users: Vec<VlessConfiguredUser>) -> Self {
        Self {
            users: Arc::new(std::sync::RwLock::new(users.into())),
        }
    }

    pub fn user_count(&self) -> usize {
        self.users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    pub fn replace_users(&self, users: Vec<VlessConfiguredUser>) {
        *self
            .users
            .write()
            .unwrap_or_else(|error| error.into_inner()) = users.into();
    }

    pub fn from_config_parts<I>(users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = VlessInboundUserConfigParts>,
    {
        users
            .into_iter()
            .map(
                |(
                    id,
                    flow,
                    principal_key,
                    up_bps,
                    down_bps,
                    device_limit,
                    quota_remaining_bytes,
                    policy_revision,
                )| {
                    VlessConfiguredUser::from_config(
                        &id,
                        flow.as_deref(),
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )
                },
            )
            .collect::<Result<Vec<_>, Error>>()
            .map(Self::from_users)
    }

    pub fn from_config_users<I, U>(users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = U>,
        U: IntoVlessInboundUserConfig,
    {
        Self::from_config_parts(users.into_iter().map(U::into_vless_inbound_user_config))
    }

    pub fn replace_config_users<I, U>(&self, users: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = U>,
        U: IntoVlessInboundUserConfig,
    {
        let replacement = users
            .into_iter()
            .map(IntoVlessInboundUserConfig::into_vless_inbound_user_config)
            .map(
                |(
                    id,
                    flow,
                    principal_key,
                    up_bps,
                    down_bps,
                    device_limit,
                    quota_remaining_bytes,
                    policy_revision,
                )| {
                    VlessConfiguredUser::from_config(
                        &id,
                        flow.as_deref(),
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )
                },
            )
            .collect::<Result<Vec<_>, Error>>()?;
        self.replace_users(replacement);
        Ok(())
    }

    pub async fn accept_tcp_with_auth_and_id<S>(
        &self,
        inbound: VlessInbound,
        stream: &mut S,
    ) -> Result<(Session, [u8; 16]), Error>
    where
        S: AsyncSocket,
    {
        let users = self
            .users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let auth = VlessConfiguredUsers::new(users.as_ref());
        inbound.accept_tcp_with_auth_and_id(stream, &auth).await
    }

    pub(crate) async fn accept_client_owned<S>(
        self,
        inbound: VlessInbound,
        mut stream: S,
    ) -> Result<VlessAcceptedClient<S>, VlessClientAcceptError<S>>
    where
        S: AsyncSocket,
    {
        let users = self
            .users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let auth = VlessConfiguredUsers::new(users.as_ref());
        match inbound
            .accept_tcp_with_auth_and_id(&mut stream, &auth)
            .await
        {
            Ok((session, user_id)) => Ok(VlessAcceptedClient::new(
                VlessAcceptedSession::new(session, user_id),
                stream,
            )),
            Err(error) => Err(VlessClientAcceptError::new(error, stream)),
        }
    }

    pub async fn accept_tcp_with_auth<S>(
        &self,
        inbound: VlessInbound,
        stream: &mut S,
    ) -> Result<Session, Error>
    where
        S: AsyncSocket,
    {
        let users = self
            .users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let auth = VlessConfiguredUsers::new(users.as_ref());
        inbound.accept_tcp_with_auth(stream, &auth).await
    }
}

pub trait IntoVlessInboundUserConfig {
    fn into_vless_inbound_user_config(self) -> VlessInboundUserConfigParts;
}

impl IntoVlessInboundUserConfig for VlessInboundUserConfigParts {
    fn into_vless_inbound_user_config(self) -> VlessInboundUserConfigParts {
        self
    }
}

#[cfg(all(feature = "runtime", feature = "reality"))]
impl IntoVlessInboundUserConfig for crate::transport::VlessInboundUserRef<'_> {
    fn into_vless_inbound_user_config(self) -> VlessInboundUserConfigParts {
        (
            self.id.to_owned(),
            self.flow.map(str::to_owned),
            self.principal_key.map(str::to_owned),
            self.up_bps,
            self.down_bps,
            self.device_limit,
            self.quota_remaining_bytes,
            self.policy_revision,
        )
    }
}

impl VlessInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("vless")
    }

    #[cfg(feature = "reality")]
    async fn accept_mux_session_with_auth<S>(
        &self,
        stream: &mut S,
        mux_master_uuid: [u8; 16],
        auth: Option<SessionAuth>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<crate::mux::VlessInboundMuxServer, Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream).await?;
        Ok(
            crate::mux::VlessInboundMuxServer::from_master_uuid_with_auth(
                mux_master_uuid,
                auth,
                mux_response_backlog,
            ),
        )
    }

    /// Accept a VLESS connection, authenticate the user, and return both
    /// the session and the raw UUID (needed for MUX stream encryption).
    pub async fn accept_tcp_with_auth_and_id<S, A>(
        &self,
        stream: &mut S,
        auth: &A,
    ) -> Result<(Session, [u8; 16]), Error>
    where
        S: AsyncSocket,
        A: VlessUserStore,
    {
        #[cfg(feature = "reality")]
        {
            let (mut session, id, flow) = read_request_with_flow(stream).await?;
            let Some(user) = auth.find_user(&id) else {
                return Err(Error::Unsupported("VLESS user is not authorized"));
            };
            validate_user_flow(user.flow, flow)?;
            let mut sa = SessionAuth::new("vless");
            sa.principal_key = user.principal_key;
            sa.up_bps = user.up_bps;
            sa.down_bps = user.down_bps;
            sa.device_limit = user.device_limit;
            sa.quota_remaining_bytes = user.quota_remaining_bytes;
            sa.policy_revision = user.policy_revision;
            session.apply_auth(sa);
            Ok((session, id))
        }
        #[cfg(not(feature = "reality"))]
        {
            let (mut session, id) = read_request(stream).await?;
            let Some(user) = auth.find_user(&id) else {
                return Err(Error::Unsupported("VLESS user is not authorized"));
            };
            let mut sa = SessionAuth::new("vless");
            sa.principal_key = user.principal_key;
            sa.up_bps = user.up_bps;
            sa.down_bps = user.down_bps;
            sa.device_limit = user.device_limit;
            sa.quota_remaining_bytes = user.quota_remaining_bytes;
            sa.policy_revision = user.policy_revision;
            session.apply_auth(sa);
            Ok((session, id))
        }
    }

    pub async fn accept_tcp_with_auth<S, A>(
        &self,
        stream: &mut S,
        auth: &A,
    ) -> Result<Session, Error>
    where
        S: AsyncSocket,
        A: VlessUserStore,
    {
        #[cfg(feature = "reality")]
        {
            let (mut session, id, flow) = read_request_with_flow(stream).await?;
            let Some(user) = auth.find_user(&id) else {
                return Err(Error::Unsupported("VLESS user is not authorized"));
            };
            validate_user_flow(user.flow, flow)?;
            let mut sa = SessionAuth::new("vless");
            sa.principal_key = user.principal_key;
            sa.up_bps = user.up_bps;
            sa.down_bps = user.down_bps;
            sa.device_limit = user.device_limit;
            sa.quota_remaining_bytes = user.quota_remaining_bytes;
            sa.policy_revision = user.policy_revision;
            session.apply_auth(sa);
            Ok(session)
        }
        #[cfg(not(feature = "reality"))]
        {
            let (mut session, id) = read_request(stream).await?;
            let Some(user) = auth.find_user(&id) else {
                return Err(Error::Unsupported("VLESS user is not authorized"));
            };
            let mut sa = SessionAuth::new("vless");
            sa.principal_key = user.principal_key;
            sa.up_bps = user.up_bps;
            sa.down_bps = user.down_bps;
            sa.device_limit = user.device_limit;
            sa.quota_remaining_bytes = user.quota_remaining_bytes;
            sa.policy_revision = user.policy_revision;
            session.apply_auth(sa);
            Ok(session)
        }
    }

    pub async fn send_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        stream
            .write_all(&[VLESS_VERSION, 0x00])
            .await
            .map_err(|_| Error::Io("failed to write VLESS response"))
    }

    pub async fn send_ok<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream).await
    }

    pub async fn send_blocked<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let _ = stream.shutdown().await;
        Ok(())
    }

    pub async fn send_upstream_failure<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let _ = stream.shutdown().await;
        Ok(())
    }

    pub async fn handshake_with_auth<S, A>(
        &self,
        stream: &mut S,
        auth: &A,
    ) -> Result<Session, Error>
    where
        S: AsyncSocket,
        A: VlessUserStore,
    {
        let session = self.accept_tcp_with_auth(stream, auth).await?;
        self.send_response(stream).await?;
        Ok(session)
    }

    pub async fn handshake_mux_with_auth<S, A>(
        &self,
        stream: &mut S,
        auth: &A,
    ) -> Result<Session, Error>
    where
        S: AsyncSocket,
        A: VlessUserStore,
    {
        let (session, id) = read_request_mux(stream).await?;
        let Some(_user) = auth.find_user(&id) else {
            return Err(Error::Unsupported("VLESS user is not authorized"));
        };
        self.send_response(stream).await?;
        Ok(session)
    }

    /// Check if a Session returned by accept_tcp_with_auth is a MUX session.
    pub fn is_mux_session(session: &Session) -> bool {
        session.port == 0 && matches!(&session.target, Address::Domain(d) if d.is_empty())
    }
}

impl<S> zero_core::InboundClientResponse<S> for VlessInbound
where
    S: AsyncSocket,
{
    async fn send_ok(&self, client: &mut S) -> Result<(), Error> {
        VlessInbound::send_ok(self, client).await
    }

    async fn send_blocked(&self, client: &mut S) -> Result<(), Error> {
        VlessInbound::send_blocked(self, client).await
    }

    async fn send_upstream_failure(&self, client: &mut S) -> Result<(), Error> {
        VlessInbound::send_upstream_failure(self, client).await
    }
}

// ── read_request (non-reality) ──

#[cfg(not(feature = "reality"))]
async fn read_request<S>(stream: &mut S) -> Result<(Session, [u8; 16]), Error>
where
    S: AsyncSocket,
{
    let mut version = [0_u8; 1];
    read_exact(stream, &mut version).await?;
    if version[0] != VLESS_VERSION {
        return Err(Error::Protocol("unsupported VLESS version"));
    }
    let mut id = [0_u8; 16];
    read_exact(stream, &mut id).await?;
    read_addon(stream).await?;
    let mut command = [0_u8; 1];
    read_exact(stream, &mut command).await?;
    if command[0] == CMD_MUX {
        return command_to_session(CMD_MUX, Address::Domain(String::new()), 0, id);
    }
    let mut port = [0_u8; 2];
    read_exact(stream, &mut port).await?;
    let port = u16::from_be_bytes(port);
    if port == 0 {
        return Err(Error::Protocol("VLESS target port must not be 0"));
    }
    let mut atyp = [0_u8; 1];
    read_exact(stream, &mut atyp).await?;
    let target = read_address(stream, atyp[0]).await?;
    command_to_session(command[0], target, port, id)
}

// ── read_request_with_flow (reality) ──

#[cfg(feature = "reality")]
async fn read_request_with_flow<S>(
    stream: &mut S,
) -> Result<(Session, [u8; 16], Option<&'static str>), Error>
where
    S: AsyncSocket,
{
    let mut version = [0_u8; 1];
    read_exact(stream, &mut version).await?;
    if version[0] != VLESS_VERSION {
        return Err(Error::Protocol("unsupported VLESS version"));
    }
    let mut id = [0_u8; 16];
    read_exact(stream, &mut id).await?;
    let (flow, zero_aead) = read_flow_addons(stream).await?;

    if zero_aead {
        let (command, port, target) =
            flow_read_request(stream, Some(FLOW_ZERO_AEAD_V1), &id).await?;
        command_to_session(command, target, port, id)
            .map(|(session, id)| (session, id, Some(FLOW_ZERO_AEAD_V1)))
    } else {
        let mut command = [0_u8; 1];
        read_exact(stream, &mut command).await?;
        if command[0] == CMD_MUX {
            return command_to_session(CMD_MUX, Address::Domain(String::new()), 0, id)
                .map(|(session, id)| (session, id, flow));
        }
        let mut port = [0_u8; 2];
        read_exact(stream, &mut port).await?;
        let port = u16::from_be_bytes(port);
        if port == 0 {
            return Err(Error::Protocol("VLESS target port must not be 0"));
        }
        let mut atyp = [0_u8; 1];
        read_exact(stream, &mut atyp).await?;
        let target = read_address(stream, atyp[0]).await?;
        command_to_session(command[0], target, port, id).map(|(session, id)| (session, id, flow))
    }
}

#[cfg(feature = "reality")]
async fn read_flow_addons<S>(stream: &mut S) -> Result<(Option<&'static str>, bool), Error>
where
    S: AsyncSocket,
{
    let mut length = [0_u8; 1];
    read_exact(stream, &mut length).await?;
    if length[0] == 1 {
        // Private Zero AEAD v1 used 0x01 as a marker before standard Addons
        // support existed. It remains available only under `zero-aead-v1`.
        return Ok((Some(FLOW_ZERO_AEAD_V1), true));
    }
    let mut encoded = vec![0_u8; length[0] as usize];
    read_exact(stream, &mut encoded).await?;
    decode_addons(&encoded).map(|flow| (flow, false))
}

#[cfg(feature = "reality")]
fn validate_user_flow(
    configured: Option<&'static str>,
    requested: Option<&'static str>,
) -> Result<(), Error> {
    if configured == requested {
        Ok(())
    } else {
        Err(Error::Unsupported(
            "VLESS requested flow does not match the authorized user",
        ))
    }
}

// ── read_request_mux (used by accept_mux_header) ──

async fn read_request_mux<S>(stream: &mut S) -> Result<(Session, [u8; 16]), Error>
where
    S: AsyncSocket,
{
    let mut version = [0_u8; 1];
    read_exact(stream, &mut version).await?;
    if version[0] != VLESS_VERSION {
        return Err(Error::Protocol("unsupported VLESS version"));
    }
    let mut id = [0_u8; 16];
    read_exact(stream, &mut id).await?;

    #[cfg(feature = "reality")]
    {
        let (flow, zero_aead) = read_flow_addons(stream).await?;
        if zero_aead {
            let (command, _port, _target) =
                flow_read_request(stream, Some(FLOW_ZERO_AEAD_V1), &id).await?;
            if command != CMD_MUX {
                return Err(Error::Protocol("VLESS MUX expected"));
            }
        } else {
            if flow.is_some() {
                return Err(Error::Unsupported(
                    "VLESS flow is not supported for MUX commands",
                ));
            }
            let mut command = [0_u8; 1];
            read_exact(stream, &mut command).await?;
            if command[0] != CMD_MUX {
                return Err(Error::Protocol("VLESS MUX expected"));
            }
        }
    }
    #[cfg(not(feature = "reality"))]
    {
        read_addon(stream).await?;
        let mut command = [0_u8; 1];
        read_exact(stream, &mut command).await?;
        if command[0] != CMD_MUX {
            return Err(Error::Protocol("VLESS MUX expected"));
        }
    }

    Ok((
        Session::new(
            0,
            Address::Domain(String::new()),
            0,
            Network::Tcp,
            ProtocolType::new("vless"),
        ),
        id,
    ))
}

// ── command dispatch helper ──

fn command_to_session(
    command: u8,
    target: Address,
    port: u16,
    id: [u8; 16],
) -> Result<(Session, [u8; 16]), Error> {
    match command {
        CMD_TCP => Ok((
            Session::new(0, target, port, Network::Tcp, ProtocolType::new("vless")),
            id,
        )),
        CMD_UDP => Ok((
            Session::new(0, target, port, Network::Udp, ProtocolType::new("vless")),
            id,
        )),
        CMD_MUX => Ok((
            Session::new(
                0,
                Address::Domain(String::new()),
                0,
                Network::Tcp,
                ProtocolType::new("vless"),
            ),
            id,
        )),
        _ => Err(Error::Unsupported("VLESS command is not supported")),
    }
}

// ── Config adapter ──
