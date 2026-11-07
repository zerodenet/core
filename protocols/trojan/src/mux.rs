#[cfg(feature = "runtime")]
pub(crate) mod native;
#[cfg(feature = "runtime")]
use zero_transport::pooled_driver::{join as join_mux_driver, Driver as MuxDriver};
#[cfg(not(feature = "runtime"))]
mod driver;
use core::future::Future;
#[cfg(not(feature = "runtime"))]
use driver::{join as join_mux_driver, Driver as MuxDriver};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, watch};
use zero_core::{
    Address, Error, InboundMuxTcpRelay, InboundMuxUdpReadFailure, InboundMuxUdpReadFailureAction,
    InboundMuxUdpRelay, InboundStreamUdpRelay, MuxUdpDecodeFailure, MuxUdpResponder, Network,
    ProtocolType, Session, SessionAuth,
};
use zero_traits::AsyncSocket;

use crate::shared::{parse_address_from_bytes, write_address};

mod client_response;
use client_response::run_mux_read_relay_with_shutdown;

mod backlog;
mod response;
#[cfg(feature = "runtime")]
mod standard;
#[cfg(feature = "runtime")]
pub(crate) use standard::source as standard_source;
#[cfg(test)]
mod tests;

pub(crate) use backlog::MuxResponseBacklogPolicy;
use backlog::{BufferedMuxResponse, MuxResponseBacklog};

pub const MUX_MAX_META_LEN: usize = 512;
pub const MUX_MAX_DATA_LEN: usize = 16 * 1024;
pub const MUX_NETWORK_TCP: u8 = 0x01;
pub const MUX_NETWORK_UDP: u8 = 0x02;
pub const MUX_STATUS_NEW: u8 = 0x01;
pub const MUX_STATUS_KEEP: u8 = 0x02;
pub const MUX_STATUS_END: u8 = 0x03;
pub const MUX_STATUS_KEEP_ALIVE: u8 = 0x04;
pub const MUX_OPTION_DATA: u8 = 0x01;
pub const MUX_OPTION_ERROR: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TrojanMuxPoolKey {
    server: String,
    port: u16,
    password: String,
    tls_server_name: Option<String>,
    insecure: bool,
    client_fingerprint: Option<String>,
    idle_timeout: Option<Duration>,
    response_backlog: MuxResponseBacklogPolicy,
}

#[cfg(feature = "runtime")]
type NativeBinding = zero_platform_tokio::contract::network::UpstreamBinding;
#[cfg(not(feature = "runtime"))]
type NativeBinding = ();
type PoolEntries =
    HashMap<(Option<Arc<str>>, TrojanMuxPoolKey, Option<NativeBinding>), Arc<TrojanMuxConn>>;

#[derive(Clone)]
pub struct TrojanMuxConnectionPool {
    #[cfg(feature = "runtime")]
    control: zero_transport::pooled_driver::PoolControl,
    pool: Arc<Mutex<PoolEntries>>,
    scope: Option<Arc<str>>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pool_key_from_config(
    server: &str,
    port: u16,
    password: &str,
    tls_server_name: Option<&str>,
    insecure: bool,
    client_fingerprint: Option<&str>,
    idle_timeout_secs: Option<u64>,
    response_backlog: MuxResponseBacklogPolicy,
) -> TrojanMuxPoolKey {
    TrojanMuxPoolKey {
        server: server.to_owned(),
        port,
        password: password.to_owned(),
        tls_server_name: tls_server_name.map(ToOwned::to_owned),
        insecure,
        client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
        idle_timeout: idle_timeout_secs.map(Duration::from_secs),
        response_backlog,
    }
}

impl TrojanMuxPoolKey {
    async fn establish_mux_outbound_stream<S>(&self, mut stream: S) -> Result<S, Error>
    where
        S: AsyncSocket,
    {
        crate::outbound::establish_outbound_mux_connection(&mut stream, &self.password).await?;
        Ok(stream)
    }

    fn into_pool_conn<S>(self, stream: S, max_concurrency: u32) -> TrojanMuxConn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        TrojanMuxConn::new(
            stream,
            max_concurrency,
            self.idle_timeout,
            self.response_backlog,
        )
    }
    #[cfg(feature = "runtime")]
    fn prepare_pool_conn<S>(self, stream: S, max_concurrency: u32) -> (TrojanMuxConn, MuxDriver)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        TrojanMuxConn::prepare(
            stream,
            max_concurrency,
            self.idle_timeout,
            self.response_backlog,
        )
    }
}

impl Default for TrojanMuxConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for TrojanMuxConnectionPool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TrojanMuxConnectionPool")
            .field(
                "entries",
                &self.pool.lock().expect("trojan mux pool poisoned").len(),
            )
            .finish()
    }
}

impl TrojanMuxConnectionPool {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(Mutex::new(HashMap::new())),
            scope: None,
            #[cfg(feature = "runtime")]
            control: Default::default(),
        }
    }

    /// A cold view of the same actual adapter pool. Scope is private protocol
    /// state, never a new pool/worker or a native resource-close receipt.
    pub(crate) fn scoped(&self, tag: &str, transport: impl std::fmt::Debug) -> Self {
        let scope: Arc<str> = format!("{tag:?}:{transport:?}").into();
        Self {
            pool: self.pool.clone(),
            #[cfg(feature = "runtime")]
            // Ownership is stable for the configured node across transport
            // changes; the actual cache scope still includes the full profile.
            control: self.control.scoped(Arc::from(tag)),
            scope: Some(scope),
        }
    }

    pub fn evict_all(&self) {
        #[cfg(feature = "runtime")]
        self.pool
            .lock()
            .expect("trojan mux pool poisoned")
            .retain(|_, connection| connection.original.is_some());
        #[cfg(not(feature = "runtime"))]
        self.pool.lock().expect("trojan mux pool poisoned").clear();
    }

    pub(crate) async fn open_tcp_stream<S, OpenStream, OpenStreamFut, E>(
        &self,
        key: TrojanMuxPoolKey,
        max_concurrency: u32,
        target: Address,
        port: u16,
        open_stream: OpenStream,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.open_stream(
            key,
            max_concurrency,
            target,
            port,
            Network::Tcp,
            open_stream,
        )
        .await
    }

    pub(crate) async fn open_udp_stream<S, OpenStream, OpenStreamFut, E>(
        &self,
        key: TrojanMuxPoolKey,
        max_concurrency: u32,
        target: Address,
        port: u16,
        open_stream: OpenStream,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.open_stream(
            key,
            max_concurrency,
            target,
            port,
            Network::Udp,
            open_stream,
        )
        .await
    }

    async fn open_stream<S, OpenStream, OpenStreamFut, E>(
        &self,
        key: TrojanMuxPoolKey,
        max_concurrency: u32,
        target: Address,
        port: u16,
        network: Network,
        open_stream: OpenStream,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Unpin + 'static, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        #[cfg(feature = "runtime")]
        self.control
            .enter_legacy()
            .map_err(|_| E::from(Error::Io("MUX pool has an installed native owner")))?;
        let (conn, session_id) = self
            .get_or_create_conn(key, max_concurrency, |key, max_concurrency| async move {
                let stream = match open_stream().await {
                    Ok(stream) => stream,
                    Err(error) => return Err(error),
                };
                let stream = match key.establish_mux_outbound_stream(stream).await {
                    Ok(stream) => stream,
                    Err(error) => return Err(E::from(error)),
                };
                Ok(key.into_pool_conn(stream, max_concurrency))
            })
            .await?;
        Ok(conn.open_reserved_stream(session_id, target, port, network))
    }

    async fn get_or_create_conn<F, Fut, E>(
        &self,
        key: TrojanMuxPoolKey,
        max_concurrency: u32,
        create_conn: F,
    ) -> Result<(Arc<TrojanMuxConn>, u16), E>
    where
        F: FnOnce(TrojanMuxPoolKey, u32) -> Fut,
        Fut: Future<Output = Result<TrojanMuxConn, E>>,
        E: From<Error>,
    {
        self.get_or_create_conn_with_binding(key, max_concurrency, None, create_conn)
            .await
    }
    async fn get_or_create_conn_with_binding<F, Fut, E>(
        &self,
        key: TrojanMuxPoolKey,
        max_concurrency: u32,
        binding: Option<NativeBinding>,
        create_conn: F,
    ) -> Result<(Arc<TrojanMuxConn>, u16), E>
    where
        F: FnOnce(TrojanMuxPoolKey, u32) -> Fut,
        Fut: Future<Output = Result<TrojanMuxConn, E>>,
        E: From<Error>,
    {
        #[cfg(feature = "runtime")]
        let retired = {
            let mut entries = self.pool.lock().expect("actual protocol MUX map");
            entries
                .extract_if(|_, connection| {
                    connection
                        .original
                        .as_ref()
                        .is_some_and(|original| original.confirmed_closed())
                })
                .map(|(_, connection)| connection)
                .collect::<Vec<_>>()
        };
        #[cfg(feature = "runtime")]
        drop(retired);
        let lookup = (self.scope.clone(), key.clone(), binding);
        let cached = self
            .pool
            .lock()
            .expect("trojan mux pool poisoned")
            .get(&lookup)
            .cloned();

        if let Some(conn) = cached {
            if let Some(session_id) = conn.try_reserve_stream_id() {
                return Ok((conn, session_id));
            }
        }

        let conn = Arc::new(create_conn(key.clone(), max_concurrency).await?);
        let session_id = conn
            .try_reserve_stream_id()
            .ok_or_else(|| E::from(Error::Io("MUX connection retired during preparation")))?;
        self.pool
            .lock()
            .expect("trojan mux pool poisoned")
            .insert(lookup, conn.clone());
        Ok((conn, session_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MuxFrame {
    pub session_id: u16,
    pub status: u8,
    pub option: u8,
    pub network: Option<Network>,
    pub target: Option<Address>,
    pub port: Option<u16>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrojanMuxServerEvent {
    KeepAlive,
    NewStream {
        session_id: u16,
        network: Network,
        target: Address,
        port: u16,
        payload: Vec<u8>,
    },
    Data {
        session_id: u16,
        payload: Vec<u8>,
    },
    End {
        session_id: u16,
    },
    Unknown {
        session_id: u16,
        status: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrojanInboundMuxAction {
    KeepAlive,
    OpenStream {
        session_id: u16,
        session: Box<Session>,
        initial_payload: Vec<u8>,
    },
    Data {
        session_id: u16,
        payload: Vec<u8>,
    },
    End {
        session_id: u16,
    },
    Unknown {
        session_id: u16,
    },
}

struct TrojanInboundMuxOpenedStream {
    session_id: u16,
    session: Box<Session>,
    up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

enum TrojanInboundMuxOpenedRouteState {
    Tcp {
        session: Box<Session>,
        relay: TrojanInboundMuxTcpRelay,
    },
    Udp {
        relay: TrojanInboundMuxUdpRelay,
    },
}

struct TrojanInboundMuxOpenedRoute {
    state: TrojanInboundMuxOpenedRouteState,
}

impl TrojanInboundMuxOpenedRoute {
    fn tcp(session: Box<Session>, relay: TrojanInboundMuxTcpRelay) -> Self {
        Self {
            state: TrojanInboundMuxOpenedRouteState::Tcp { session, relay },
        }
    }

    fn udp(relay: TrojanInboundMuxUdpRelay) -> Self {
        Self {
            state: TrojanInboundMuxOpenedRouteState::Udp { relay },
        }
    }
}

pub struct TrojanInboundMuxTcpRelay {
    session_id: u16,
    up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    writer: TrojanInboundMuxWriter,
}

impl TrojanInboundMuxTcpRelay {
    #[cfg(feature = "runtime")]
    pub(crate) fn into_standard_stream(
        self,
    ) -> impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {
        let sid = self.session_id;
        let output = self.writer.clone();
        let sender = output.write_tx.clone();
        let mut waiter = None;
        let header = encode_keep_stream(sid, &[])
            .expect("fixed TCP header")
            .len();
        let end_size = encode_end_stream(sid).expect("fixed TCP END").len();
        let maximum = output.backlog.payload_limit(MUX_MAX_DATA_LEN, header);
        zero_transport::pooled_driver::ExclusiveIo::new(
            zero_transport::stream::QueuedChannelStream::new(
                self.up_rx,
                sender,
                move |cx, bytes: Option<&[u8]>| {
                    let len = match bytes {
                        Some(bytes) => bytes.len() + header,
                        None => end_size,
                    };
                    let credit =
                        std::task::ready!(output.backlog.poll_charge(cx, len, &mut waiter))
                            .ok_or_else(|| {
                                io::Error::other(
                                    "Trojan MUX frame exceeds its original byte budget",
                                )
                            })?;
                    let frame = match bytes {
                        Some(bytes) => encode_keep_stream(sid, bytes),
                        None => encode_end_stream(sid),
                    }
                    .map_err(io::Error::other)?;
                    Poll::Ready(Ok(credit.map(|()| frame)))
                },
                move || {
                    self.writer.request_end(sid);
                },
                maximum,
                Vec::new(),
            ),
        )
    }
    fn new(
        session_id: u16,
        up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        writer: TrojanInboundMuxWriter,
    ) -> Self {
        Self {
            session_id,
            up_rx,
            writer,
        }
    }

    async fn relay_stream<S>(self, upstream: S)
    where
        S: AsyncSocket + 'static,
        S::Error: Send,
    {
        relay_inbound_mux_stream(self.session_id, self.up_rx, self.writer, upstream).await;
    }
}

impl InboundMuxTcpRelay for TrojanInboundMuxTcpRelay {
    fn mux_session_id(&self) -> u16 {
        self.session_id
    }

    fn close_stream(&self) -> impl core::future::Future<Output = ()> + Send {
        let session_id = self.session_id;
        let writer = self.writer.clone();
        async move {
            let _ = writer.end_inbound_stream(session_id);
        }
    }

    async fn relay_stream<S>(self, upstream: S)
    where
        S: AsyncSocket + 'static,
        S::Error: Send,
    {
        TrojanInboundMuxTcpRelay::relay_stream(self, upstream).await;
    }
}

impl TrojanInboundMuxOpenedStream {
    fn new(
        session_id: u16,
        session: Box<Session>,
        up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> Self {
        Self {
            session_id,
            session,
            up_rx,
        }
    }

    fn into_parts(self) -> (u16, Session, mpsc::UnboundedReceiver<Vec<u8>>) {
        (self.session_id, *self.session, self.up_rx)
    }

    fn into_route_with_auth(
        self,
        writer: TrojanInboundMuxWriter,
        auth: Option<&SessionAuth>,
    ) -> TrojanInboundMuxOpenedRoute {
        let (session_id, mut session, up_rx) = self.into_parts();
        if let Some(auth) = auth {
            session.apply_auth(auth.clone());
        }
        match session.network {
            Network::Tcp => TrojanInboundMuxOpenedRoute::tcp(
                Box::new(session),
                TrojanInboundMuxTcpRelay::new(session_id, up_rx, writer),
            ),
            Network::Udp => {
                let port = session.port;
                let target = session.target;
                TrojanInboundMuxOpenedRoute::udp(TrojanInboundMuxUdpRelay::new(
                    session_id,
                    up_rx,
                    crate::udp::TrojanInboundMuxUdpResponder::new(target, port, writer, session_id),
                    auth.cloned(),
                ))
            }
        }
    }
}

pub struct TrojanInboundMuxUdpRelay {
    session_id: u16,
    up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    responder: crate::udp::TrojanInboundMuxUdpResponder,
    auth: Option<SessionAuth>,
}

impl TrojanInboundMuxUdpRelay {
    fn new(
        session_id: u16,
        up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        responder: crate::udp::TrojanInboundMuxUdpResponder,
        auth: Option<SessionAuth>,
    ) -> Self {
        Self {
            session_id,
            up_rx,
            responder,
            auth,
        }
    }
}

#[async_trait::async_trait]
impl InboundMuxUdpRelay for TrojanInboundMuxUdpRelay {
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<Option<zero_core::InboundUdpDispatch>, InboundMuxUdpReadFailure> {
        let Some(payload) = self.up_rx.recv().await else {
            return Ok(None);
        };
        if payload.is_empty() {
            return Ok(None);
        }

        match self.responder.decode_inbound_dispatch(&payload) {
            Ok(inbound_dispatch) => Ok(Some(inbound_dispatch)),
            Err(error) => Err(InboundMuxUdpReadFailure {
                error,
                action: match self.responder.decode_failure() {
                    MuxUdpDecodeFailure::Continue => InboundMuxUdpReadFailureAction::Continue,
                    MuxUdpDecodeFailure::End => InboundMuxUdpReadFailureAction::End,
                },
            }),
        }
    }

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.responder
            .write_response_for_target(target, port, payload)
    }

    fn end_inbound_stream(&mut self) -> Result<usize, Error> {
        self.responder.end_inbound_stream()
    }

    fn mux_session_id(&self) -> u16 {
        self.session_id
    }

    fn auth(&self) -> Option<&SessionAuth> {
        self.auth.as_ref()
    }
}

fn is_mux_cool_session(session: &Session) -> bool {
    matches!(&session.target, Address::Domain(domain) if domain == crate::shared::MUX_COOL_DOMAIN)
        && session.port == crate::shared::MUX_COOL_PORT
        && session.network == Network::Tcp
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrojanInboundSessionKind {
    Tcp,
    Udp,
    Mux,
}

enum TrojanInboundAcceptedStreamState<S> {
    Tcp {
        session: Session,
        stream: S,
    },
    Udp {
        session: Session,
        relay: TrojanInboundUdpRelay<S>,
    },
    Mux {
        reader: tokio::io::ReadHalf<S>,
        mux_server: TrojanInboundMuxServer,
    },
}

pub struct TrojanInboundAcceptedStream<S> {
    state: TrojanInboundAcceptedStreamState<S>,
}

pub struct TrojanInboundUdpRelay<S> {
    stream: S,
    responder: crate::udp::TrojanInboundUdpResponder,
    auth: Option<SessionAuth>,
}

fn classify_inbound_session(session: &Session) -> TrojanInboundSessionKind {
    match session.network {
        Network::Udp => TrojanInboundSessionKind::Udp,
        Network::Tcp if is_mux_cool_session(session) => TrojanInboundSessionKind::Mux,
        Network::Tcp => TrojanInboundSessionKind::Tcp,
    }
}

impl<S> TrojanInboundUdpRelay<S> {
    fn new(
        stream: S,
        responder: crate::udp::TrojanInboundUdpResponder,
        auth: Option<SessionAuth>,
    ) -> Self {
        Self {
            stream,
            responder,
            auth,
        }
    }

    fn into_parts(
        self,
    ) -> (
        S,
        crate::udp::TrojanInboundUdpResponder,
        Option<SessionAuth>,
    ) {
        (self.stream, self.responder, self.auth)
    }
}

impl<S> InboundStreamUdpRelay for TrojanInboundUdpRelay<S>
where
    S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    type Stream = S;
    type Responder = crate::udp::TrojanInboundUdpResponder;

    fn into_stream_udp_parts(self) -> (Self::Stream, Self::Responder, Option<SessionAuth>) {
        self.into_parts()
    }
}

impl<S> TrojanInboundAcceptedStream<S> {
    fn tcp(session: Session, stream: S) -> Self {
        Self {
            state: TrojanInboundAcceptedStreamState::Tcp { session, stream },
        }
    }

    fn udp(session: Session, relay: TrojanInboundUdpRelay<S>) -> Self {
        Self {
            state: TrojanInboundAcceptedStreamState::Udp { session, relay },
        }
    }

    fn mux(reader: tokio::io::ReadHalf<S>, mux_server: TrojanInboundMuxServer) -> Self {
        Self {
            state: TrojanInboundAcceptedStreamState::Mux { reader, mux_server },
        }
    }

    pub(crate) fn from_session_stream(
        session: Session,
        stream: S,
        mux_response_backlog: MuxResponseBacklogPolicy,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        match classify_inbound_session(&session) {
            TrojanInboundSessionKind::Tcp => Self::tcp(session, stream),
            TrojanInboundSessionKind::Udp => {
                let responder = crate::inbound::TrojanInbound.accept_udp_session();
                let auth = session.auth.clone();
                Self::udp(session, TrojanInboundUdpRelay::new(stream, responder, auth))
            }
            TrojanInboundSessionKind::Mux => {
                let auth = session.auth.clone();
                let (reader, writer) = tokio::io::split(stream);
                Self::mux(
                    reader,
                    crate::inbound::TrojanInbound.accept_mux_session_from_tokio_writer(
                        writer,
                        auth,
                        mux_response_backlog,
                    ),
                )
            }
        }
    }

    async fn dispatch<Tcp, TcpFut, Udp, UdpFut, Mux, MuxFut, E>(
        self,
        tcp: Tcp,
        udp: Udp,
        mux: Mux,
    ) -> Result<(), E>
    where
        Tcp: FnOnce(Session, S) -> TcpFut,
        TcpFut: core::future::Future<Output = Result<(), E>>,
        Udp: FnOnce(Session, TrojanInboundUdpRelay<S>) -> UdpFut,
        UdpFut: core::future::Future<Output = Result<(), E>>,
        Mux: FnOnce(tokio::io::ReadHalf<S>, TrojanInboundMuxServer) -> MuxFut,
        MuxFut: core::future::Future<Output = Result<(), E>>,
    {
        match self.state {
            TrojanInboundAcceptedStreamState::Tcp { session, stream } => tcp(session, stream).await,
            TrojanInboundAcceptedStreamState::Udp { session, relay } => udp(session, relay).await,
            TrojanInboundAcceptedStreamState::Mux { reader, mux_server } => {
                mux(reader, mux_server).await
            }
        }
    }
}

#[async_trait::async_trait]
impl<S> zero_core::InboundMuxStreamRoute for TrojanInboundAcceptedStream<S>
where
    S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type TcpStream = S;
    type UdpRelay = TrojanInboundUdpRelay<S>;
    type MuxReader = tokio::io::ReadHalf<S>;
    type MuxServer = TrojanInboundMuxServer;

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
        self.dispatch(on_tcp, on_udp, on_mux).await
    }
}

fn encode_frame(
    session_id: u16,
    status: u8,
    option: u8,
    target: Option<(&Address, u16, Network)>,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut meta = Vec::new();
    meta.extend_from_slice(&session_id.to_be_bytes());
    meta.push(status);
    meta.push(option);

    if status == MUX_STATUS_NEW {
        let Some((address, port, network)) = target else {
            return Err(Error::Protocol("trojan mux new frame requires target"));
        };
        match network {
            Network::Tcp => meta.push(MUX_NETWORK_TCP),
            Network::Udp => meta.push(MUX_NETWORK_UDP),
        }
        meta.extend_from_slice(&port.to_be_bytes());
        write_address(&mut meta, address)?;
    }

    if meta.len() > MUX_MAX_META_LEN {
        return Err(Error::Protocol("trojan mux metadata too large"));
    }

    let mut frame = Vec::with_capacity(2 + meta.len() + 2 + payload.len());
    frame.extend_from_slice(&(meta.len() as u16).to_be_bytes());
    frame.extend_from_slice(&meta);
    if option & MUX_OPTION_DATA != 0 {
        if payload.len() > MUX_MAX_DATA_LEN {
            return Err(Error::Protocol("trojan mux payload too large"));
        }
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        frame.extend_from_slice(payload);
    }
    Ok(frame)
}

async fn read_frame_from_tokio<R: AsyncRead + Unpin>(reader: &mut R) -> Result<MuxFrame, Error> {
    response::read_frame(reader).await
}

async fn read_mux_stream_frame<R>(reader: &mut R) -> Result<MuxFrame, Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    read_frame_from_tokio(reader).await
}

async fn read_mux_server_event<R>(reader: &mut R) -> Result<TrojanMuxServerEvent, Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    read_mux_stream_frame(reader).await?.try_into_server_event()
}

#[derive(Debug, Default, Clone, Copy)]
struct TrojanInboundMuxSession;

#[derive(Debug, Default)]
struct TrojanInboundMuxStreams {
    streams: std::collections::HashMap<u16, mpsc::UnboundedSender<Vec<u8>>>,
}

impl TrojanInboundMuxStreams {
    fn new() -> Self {
        Self::default()
    }

    fn open_stream(
        &mut self,
        session_id: u16,
        initial_payload: Vec<u8>,
    ) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        self.streams.insert(session_id, tx.clone());
        if !initial_payload.is_empty() {
            let _ = tx.send(initial_payload);
        }
        rx
    }

    fn push_stream_data(&self, session_id: u16, payload: Vec<u8>) -> bool {
        if payload.is_empty() {
            return true;
        }
        self.streams
            .get(&session_id)
            .is_some_and(|tx| tx.send(payload).is_ok())
    }

    fn close_inbound_stream(&mut self, session_id: u16) -> bool {
        // END drains accepted upload DATA before EOF; the target may still
        // send its response through this same logical stream and writer.
        self.streams.remove(&session_id).is_some()
    }

    fn apply_inbound_action(
        &mut self,
        action: TrojanInboundMuxAction,
    ) -> Option<TrojanInboundMuxOpenedStream> {
        match action {
            TrojanInboundMuxAction::KeepAlive => None,
            TrojanInboundMuxAction::OpenStream {
                session_id,
                session,
                initial_payload,
            } => {
                let up_rx = self.open_stream(session_id, initial_payload);
                Some(TrojanInboundMuxOpenedStream::new(
                    session_id, session, up_rx,
                ))
            }
            TrojanInboundMuxAction::Data {
                session_id,
                payload,
            } => {
                let _ = self.push_stream_data(session_id, payload);
                None
            }
            TrojanInboundMuxAction::End { session_id } => {
                let _ = self.close_inbound_stream(session_id);
                None
            }
            TrojanInboundMuxAction::Unknown { .. } => None,
        }
    }
}

async fn relay_inbound_mux_stream<S>(
    session_id: u16,
    mut up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    writer: TrojanInboundMuxWriter,
    mut upstream: S,
) where
    S: AsyncSocket + 'static,
    S::Error: Send,
{
    let mux_session = TrojanInboundMuxSession::new();
    let mut buf = vec![0_u8; MUX_MAX_DATA_LEN];
    let mut upload_open = true;
    loop {
        tokio::select! {
            payload = up_rx.recv(), if upload_open => {
                match payload {
                    Some(payload) => {
                        if payload.is_empty() {
                            break;
                        }
                        if upstream.write_all(&payload).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        upload_open = false;
                        let _ = upstream.shutdown().await;
                    }
                }
            }
            read = upstream.read(&mut buf) => {
                match read {
                    Ok(0) => break,
                    Ok(n) => {
                        if mux_session
                            .write_inbound_stream_payload(&writer, session_id, &buf[..n])
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    let _ = mux_session.write_inbound_stream_payload(&writer, session_id, &[]);
}

pub struct TrojanInboundMuxServer {
    session: TrojanInboundMuxSession,
    streams: TrojanInboundMuxStreams,
    writer: TrojanInboundMuxWriter,
    auth: Option<SessionAuth>,
}

impl TrojanInboundMuxServer {
    fn from_tokio_writer<W>(
        writer: W,
        auth: Option<SessionAuth>,
        mux_response_backlog: MuxResponseBacklogPolicy,
    ) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self {
            session: TrojanInboundMuxSession::new(),
            streams: TrojanInboundMuxStreams::new(),
            writer: TrojanInboundMuxWriter::from_tokio_writer(writer, mux_response_backlog),
            auth,
        }
    }

    async fn read_opened_stream<R>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<TrojanInboundMuxOpenedStream>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let action = self.session.read_inbound_action(reader).await?;
        Ok(self.streams.apply_inbound_action(action))
    }

    async fn next_opened_route<R>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<TrojanInboundMuxOpenedRoute>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let writer = self.writer();
        let auth = self.auth.clone();
        self.read_opened_stream(reader)
            .await
            .map(|opened| opened.map(|opened| opened.into_route_with_auth(writer, auth.as_ref())))
    }

    fn writer(&self) -> TrojanInboundMuxWriter {
        self.writer.clone()
    }
}

#[async_trait::async_trait]
impl<R> zero_core::InboundMuxServer<R> for TrojanInboundMuxServer
where
    R: tokio::io::AsyncRead + Unpin + Send,
{
    type TcpRelay = TrojanInboundMuxTcpRelay;
    type UdpRelay = TrojanInboundMuxUdpRelay;

    fn auth(&self) -> Option<&SessionAuth> {
        self.auth.as_ref()
    }

    async fn dispatch_next_opened_route<E, FTcp, FUdp>(
        &mut self,
        reader: &mut R,
        on_tcp_opened: FTcp,
        on_udp_opened: FUdp,
    ) -> Result<bool, E>
    where
        E: From<Error>,
        FTcp: FnOnce(Session, Self::TcpRelay) -> Result<(), E> + Send,
        FUdp: FnOnce(Self::UdpRelay) -> Result<(), E> + Send,
    {
        let Some(route) = self.next_opened_route(reader).await? else {
            return Ok(true);
        };

        match route.state {
            TrojanInboundMuxOpenedRouteState::Tcp { session, relay } => {
                on_tcp_opened(*session, relay)?;
            }
            TrojanInboundMuxOpenedRouteState::Udp { relay } => {
                on_udp_opened(relay)?;
            }
        }

        Ok(true)
    }
}

impl crate::inbound::TrojanInbound {
    fn accept_mux_session_from_tokio_writer<W>(
        &self,
        writer: W,
        auth: Option<SessionAuth>,
        mux_response_backlog: MuxResponseBacklogPolicy,
    ) -> TrojanInboundMuxServer
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        TrojanInboundMuxServer::from_tokio_writer(writer, auth, mux_response_backlog)
    }
}

impl TrojanInboundMuxSession {
    fn new() -> Self {
        Self
    }

    async fn next_action<R>(&self, reader: &mut R) -> Result<TrojanInboundMuxAction, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        read_mux_server_event(reader).await.map(Into::into)
    }

    async fn read_inbound_action<R>(&self, reader: &mut R) -> Result<TrojanInboundMuxAction, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.next_action(reader).await
    }

    fn write_data(
        &self,
        writer: &TrojanInboundMuxWriter,
        session_id: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        writer.data(session_id, payload)
    }

    fn write_inbound_stream_data(
        &self,
        writer: &TrojanInboundMuxWriter,
        session_id: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.write_data(writer, session_id, payload)
    }

    fn write_inbound_stream_payload(
        &self,
        writer: &TrojanInboundMuxWriter,
        session_id: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        if payload.is_empty() {
            self.end_inbound_stream(writer, session_id)
        } else {
            self.write_inbound_stream_data(writer, session_id, payload)
        }
    }

    fn write_end(&self, writer: &TrojanInboundMuxWriter, session_id: u16) -> Result<usize, Error> {
        writer.end(session_id)
    }

    fn end_inbound_stream(
        &self,
        writer: &TrojanInboundMuxWriter,
        session_id: u16,
    ) -> Result<usize, Error> {
        self.write_end(writer, session_id)
    }
}

#[derive(Clone)]
pub(crate) struct TrojanInboundMuxWriter {
    write_tx: mpsc::Sender<BufferedMuxResponse<Vec<u8>>>,
    backlog: MuxResponseBacklog,
    #[cfg(feature = "runtime")]
    pending_ends: std::sync::Arc<std::sync::Mutex<std::collections::BTreeSet<u16>>>,
    #[cfg(feature = "runtime")]
    end_changed: std::sync::Arc<tokio::sync::Notify>,
}

impl TrojanInboundMuxWriter {
    #[cfg(feature = "runtime")]
    fn prepare<W>(
        mut writer: W,
        policy: MuxResponseBacklogPolicy,
    ) -> (
        Self,
        impl Future<Output = Result<(), zero_platform_tokio::contract::ChannelError>> + Send + 'static,
    )
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (write_tx, mut write_rx) =
            mpsc::channel::<BufferedMuxResponse<Vec<u8>>>(policy.frames());
        let original = Self::new(write_tx, MuxResponseBacklog::from_policy(policy));
        let pending_ends = original.pending_ends.clone();
        let end_changed = original.end_changed.clone();
        let sender = original.write_tx.downgrade();
        let backlog = original.backlog.clone();
        (original, async move {
            loop {
                // Weak sender does not keep this original FIFO open forever.
                if let Some(write_tx) = sender.upgrade() {
                    Self {
                        write_tx,
                        backlog: backlog.clone(),
                        pending_ends: pending_ends.clone(),
                        end_changed: end_changed.clone(),
                    }
                    .retry_end_requests();
                }
                let frame = tokio::select! {
                    frame = write_rx.recv() => match frame { Some(frame) => frame, None => break },
                    _ = end_changed.notified() => continue,
                };
                let frame = frame.into_inner();
                writer
                    .write_all(&frame)
                    .await
                    .map_err(standard::write_error)?;
                writer.flush().await.map_err(standard::write_error)?;
            }
            // No strong sender remains and the original FIFO is fully drained.
            // The same original driver still owns any cancelled END facts.
            pending_end::finish_last_sender(&pending_ends, &mut writer).await?;
            writer.shutdown().await.map_err(standard::write_error)
        })
    }
    fn new(
        write_tx: mpsc::Sender<BufferedMuxResponse<Vec<u8>>>,
        backlog: MuxResponseBacklog,
    ) -> Self {
        Self {
            write_tx,
            backlog,
            #[cfg(feature = "runtime")]
            pending_ends: Default::default(),
            #[cfg(feature = "runtime")]
            end_changed: Default::default(),
        }
    }

    fn from_tokio_writer<W>(writer: W, policy: MuxResponseBacklogPolicy) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (write_tx, write_rx) = mpsc::channel(policy.frames());
        spawn_mux_write_relay(writer, write_rx);
        Self::new(write_tx, MuxResponseBacklog::from_policy(policy))
    }

    pub(crate) fn data(&self, session_id: u16, payload: &[u8]) -> Result<usize, Error> {
        let frame = encode_keep_stream(session_id, payload)?;
        self.frame(frame)?;
        Ok(payload.len())
    }

    pub(crate) fn end(&self, session_id: u16) -> Result<usize, Error> {
        self.frame(encode_end_stream(session_id)?)?;
        Ok(0)
    }

    pub(crate) fn end_inbound_stream(&self, session_id: u16) -> Result<usize, Error> {
        self.end(session_id)
    }

    pub(crate) fn frame(&self, frame: Vec<u8>) -> Result<usize, Error> {
        let len = frame.len();
        let response = self
            .backlog
            .try_buffer(len, frame)
            .map_err(|_| Error::Io("Trojan MUX response backlog byte limit exceeded"))?;
        self.write_tx
            .try_send(response)
            .map_err(|_| Error::Io("Trojan MUX response backlog frame limit exceeded"))?;
        Ok(len)
    }
}

fn decode_metadata(meta: &[u8]) -> Result<MuxFrame, Error> {
    if meta.len() < 4 {
        return Err(Error::Protocol("trojan mux metadata too short"));
    }

    let session_id = u16::from_be_bytes([meta[0], meta[1]]);
    let status = meta[2];
    let option = meta[3];

    let mut frame = MuxFrame {
        session_id,
        status,
        option,
        network: None,
        target: None,
        port: None,
        payload: Vec::new(),
    };

    if status == MUX_STATUS_NEW {
        if meta.len() < 8 {
            return Err(Error::Protocol("trojan mux new metadata too short"));
        }
        frame.network = match meta[4] {
            MUX_NETWORK_TCP => Some(Network::Tcp),
            MUX_NETWORK_UDP => Some(Network::Udp),
            _ => return Err(Error::Protocol("trojan mux unknown network")),
        };
        frame.port = Some(u16::from_be_bytes([meta[5], meta[6]]));
        frame.target = Some(parse_address_from_bytes(meta[7], &meta[8..])?);
    }

    Ok(frame)
}

fn encode_open_stream_with_network(
    session_id: u16,
    target: &Address,
    port: u16,
    network: Network,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let option = if payload.is_empty() {
        0
    } else {
        MUX_OPTION_DATA
    };
    encode_frame(
        session_id,
        MUX_STATUS_NEW,
        option,
        Some((target, port, network)),
        payload,
    )
}

pub(crate) fn encode_keep_stream(session_id: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
    encode_frame(session_id, MUX_STATUS_KEEP, MUX_OPTION_DATA, None, payload)
}

fn encode_end_stream(session_id: u16) -> Result<Vec<u8>, Error> {
    encode_frame(session_id, MUX_STATUS_END, 0, None, &[])
}

impl MuxFrame {
    fn try_into_server_event(self) -> Result<TrojanMuxServerEvent, Error> {
        match self.status {
            MUX_STATUS_KEEP_ALIVE => Ok(TrojanMuxServerEvent::KeepAlive),
            MUX_STATUS_NEW => {
                let network = self
                    .network
                    .ok_or(Error::Protocol("trojan mux new frame missing network"))?;
                let target = self
                    .target
                    .ok_or(Error::Protocol("trojan mux new frame missing target"))?;
                let port = self
                    .port
                    .ok_or(Error::Protocol("trojan mux new frame missing port"))?;
                Ok(TrojanMuxServerEvent::NewStream {
                    session_id: self.session_id,
                    network,
                    target,
                    port,
                    payload: self.payload,
                })
            }
            MUX_STATUS_KEEP => Ok(TrojanMuxServerEvent::Data {
                session_id: self.session_id,
                payload: self.payload,
            }),
            MUX_STATUS_END => Ok(TrojanMuxServerEvent::End {
                session_id: self.session_id,
            }),
            status => Ok(TrojanMuxServerEvent::Unknown {
                session_id: self.session_id,
                status,
            }),
        }
    }
}

impl From<TrojanMuxServerEvent> for TrojanInboundMuxAction {
    fn from(event: TrojanMuxServerEvent) -> Self {
        match event {
            TrojanMuxServerEvent::KeepAlive => Self::KeepAlive,
            TrojanMuxServerEvent::NewStream {
                session_id,
                network,
                target,
                port,
                payload,
            } => Self::OpenStream {
                session_id,
                session: Box::new(Session::new(
                    0,
                    target,
                    port,
                    network,
                    ProtocolType::new("trojan"),
                )),
                initial_payload: payload,
            },
            TrojanMuxServerEvent::Data {
                session_id,
                payload,
            } => Self::Data {
                session_id,
                payload,
            },
            TrojanMuxServerEvent::End { session_id } => Self::End { session_id },
            TrojanMuxServerEvent::Unknown { session_id, .. } => Self::Unknown { session_id },
        }
    }
}

struct TrojanMuxStream {
    session_id: u16,
    target: Address,
    port: u16,
    network: Network,
    #[cfg(feature = "runtime")]
    native_writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    read_rx: mpsc::Receiver<TrojanMuxDownlink>,
    write_buf: Vec<u8>,
    write_pos: usize,
    read_buf: Option<BufferedMuxResponse<Vec<u8>>>,
    read_pos: usize,
    opened: bool,
    ended: bool,
    conn: Option<Arc<TrojanMuxConn>>,
}

struct TrojanMuxConn {
    #[cfg(feature = "runtime")]
    original: Option<Arc<zero_transport::pooled_driver::Physical>>,
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    #[cfg(feature = "runtime")]
    native_udp_tx: zero_transport::pooled_driver::datagram_queue::PacketQueue,
    streams: Arc<Mutex<std::collections::HashMap<u16, mpsc::Sender<TrojanMuxDownlink>>>>,
    next_id: Mutex<u16>,
    active: Arc<Mutex<usize>>,
    max_concurrency: u32,
    closed: Arc<AtomicBool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
    response_backlog_frames: usize,
}

enum TrojanMuxDownlink {
    Data(BufferedMuxResponse<Vec<u8>>),
    Overflow,
}

impl TrojanMuxConn {
    fn new<S>(
        stream: S,
        max_concurrency: u32,
        idle_timeout: Option<Duration>,
        response_backlog_policy: MuxResponseBacklogPolicy,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, driver) = Self::prepare(
            stream,
            max_concurrency,
            idle_timeout,
            response_backlog_policy,
        );
        tokio::spawn(driver);
        connection
    }

    fn prepare<S>(
        stream: S,
        max_concurrency: u32,
        idle_timeout: Option<Duration>,
        response_backlog_policy: MuxResponseBacklogPolicy,
    ) -> (Self, MuxDriver)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (reader, writer) = tokio::io::split(stream);
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        #[cfg(feature = "runtime")]
        let (native_udp_tx, native_udp_rx) =
            zero_transport::pooled_driver::datagram_queue::channel_with_limits(
                8,
                (max_concurrency as usize).max(1).min(u16::MAX as usize),
                65535 + 1024,
            )
            .expect("bounded actual MUX FIFO limits");
        let streams = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let response_backlog = MuxResponseBacklog::from_policy(response_backlog_policy);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (activity_tx, activity_rx) = if idle_timeout.is_some() {
            let (tx, rx) = watch::channel(tokio::time::Instant::now());
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let idle_shutdown = shutdown_rx.clone();
        let idle_closed = closed.clone();
        let idle_tx = shutdown_tx.clone();
        let idle = async move {
            if let (Some(timeout), Some(activity)) = (idle_timeout, activity_rx) {
                run_mux_idle_monitor(timeout, activity, idle_closed, idle_tx, idle_shutdown).await;
            }
        };
        let write = run_mux_write_relay_with_shutdown(
            writer,
            write_rx,
            #[cfg(feature = "runtime")]
            native_udp_rx,
            closed.clone(),
            shutdown_tx.clone(),
            shutdown_rx.clone(),
            activity_tx.clone(),
        );
        let read = run_mux_read_relay_with_shutdown(
            reader,
            streams.clone(),
            closed.clone(),
            shutdown_tx.clone(),
            shutdown_rx,
            activity_tx.clone(),
            response_backlog,
        );
        let stopping = closed.clone();
        let stop = shutdown_tx.clone();
        let outputs = streams.clone();
        let driver = join_mux_driver(read, write, idle, move || {
            stopping.store(true, Ordering::Release);
            let _ = stop.send(true);
            outputs.lock().unwrap().clear();
        });

        (
            Self {
                #[cfg(feature = "runtime")]
                original: None,
                write_tx,
                #[cfg(feature = "runtime")]
                native_udp_tx,
                streams,
                next_id: Mutex::new(1),
                active: Arc::new(Mutex::new(0)),
                max_concurrency,
                closed,
                activity_tx,
                response_backlog_frames: response_backlog_policy.frames(),
            },
            driver,
        )
    }

    fn open_reserved_stream(
        self: &Arc<Self>,
        session_id: u16,
        target: Address,
        port: u16,
        network: Network,
    ) -> TrojanMuxStream {
        let (down_tx, down_rx) = mpsc::channel(self.response_backlog_frames);
        self.streams.lock().unwrap().insert(session_id, down_tx);

        TrojanMuxStream::new_with_network(
            session_id,
            target,
            port,
            network,
            self.write_tx.clone(),
            down_rx,
            self.clone(),
        )
    }

    fn try_reserve_stream_id(&self) -> Option<u16> {
        let streams = self.streams.lock().unwrap();
        let mut active = self.active.lock().unwrap();
        if self.closed.load(Ordering::Acquire)
            || *active >= self.max_concurrency.min(u16::MAX as u32) as usize
        {
            return None;
        }
        let session_id = loop {
            let mut next = self.next_id.lock().unwrap();
            let id = *next;
            *next = next.wrapping_add(1);
            if *next == 0 {
                *next = 1;
            }
            drop(next);
            if !streams.contains_key(&id) {
                break id;
            }
        };
        *active += 1;
        self.touch_idle();
        Some(session_id)
    }

    fn release_stream(self: &Arc<Self>, session_id: u16) {
        self.streams.lock().unwrap().remove(&session_id);
        let mut active = self.active.lock().unwrap();
        *active = active.saturating_sub(1);
    }

    fn touch_idle(&self) {
        if let Some(activity_tx) = &self.activity_tx {
            activity_tx.send_replace(tokio::time::Instant::now());
        }
    }
}

fn spawn_mux_write_relay<W>(
    mut writer: W,
    mut write_rx: mpsc::Receiver<BufferedMuxResponse<Vec<u8>>>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(frame) = write_rx.recv().await {
            let frame = frame.into_inner();
            if writer.write_all(&frame).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });
}

async fn run_mux_write_relay_with_shutdown<W>(
    mut writer: W,
    mut write_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    #[cfg(feature = "runtime")] mut native_udp_rx: mpsc::Receiver<
        zero_transport::pooled_driver::datagram_queue::QueuedPacket,
    >,
    closed: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            frame = async {
                #[cfg(feature = "runtime")]
                { tokio::select! { frame=write_rx.recv()=>frame, frame=native_udp_rx.recv()=>frame.map(|frame|frame.into_bytes()) } }
                #[cfg(not(feature = "runtime"))]
                { write_rx.recv().await }
            } => {
                let Some(frame) = frame else {
                    break;
                };
                if writer.write_all(&frame).await.is_err() {
                    break;
                }
                if writer.flush().await.is_err() {
                    break;
                }
                if let Some(activity_tx) = &activity_tx {
                    activity_tx.send_replace(tokio::time::Instant::now());
                }
            }
        }
    }
    let _ = writer.shutdown().await;
    closed.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
}

async fn run_mux_idle_monitor(
    idle_timeout: Duration,
    mut activity_rx: watch::Receiver<tokio::time::Instant>,
    closed: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        let deadline = *activity_rx.borrow() + idle_timeout;
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() { return; }
            }
            changed = activity_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                if tokio::time::Instant::now() < *activity_rx.borrow() + idle_timeout {
                    continue;
                }
                if !closed.swap(true, Ordering::AcqRel) {
                    let _ = shutdown_tx.send(true);
                }
                return;
            }
        }
    }
}

impl TrojanMuxStream {
    fn new_with_network(
        session_id: u16,
        target: Address,
        port: u16,
        network: Network,
        write_tx: mpsc::UnboundedSender<Vec<u8>>,
        read_rx: mpsc::Receiver<TrojanMuxDownlink>,
        conn: Arc<TrojanMuxConn>,
    ) -> Self {
        Self {
            session_id,
            target,
            port,
            network,
            #[cfg(feature = "runtime")]
            native_writer: None,
            write_tx,
            read_rx,
            write_buf: Vec::new(),
            write_pos: 0,
            read_buf: None,
            read_pos: 0,
            opened: false,
            ended: false,
            conn: Some(conn),
        }
    }

    fn queue_frame(&mut self, payload: &[u8]) -> io::Result<usize> {
        let take = payload.len().min(MUX_MAX_DATA_LEN);
        let frame = if self.opened {
            encode_keep_stream(self.session_id, &payload[..take])
        } else {
            self.opened = true;
            encode_open_stream_with_network(
                self.session_id,
                &self.target,
                self.port,
                self.network,
                &payload[..take],
            )
        }
        .map_err(protocol_error)?;
        #[cfg(feature = "runtime")]
        if let Some(writer) = &mut self.native_writer {
            writer.send(frame)?;
            return Ok(take);
        }
        self.write_tx
            .send(frame)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "trojan mux writer closed"))?;
        Ok(take)
    }

    fn flush_pending(&mut self) -> io::Result<()> {
        if self.write_pos < self.write_buf.len() {
            self.write_tx
                .send(self.write_buf[self.write_pos..].to_vec())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "trojan mux writer closed")
                })?;
            self.write_pos = self.write_buf.len();
        }
        self.write_buf.clear();
        self.write_pos = 0;
        Ok(())
    }
}

impl Drop for TrojanMuxStream {
    fn drop(&mut self) {
        #[cfg(feature = "runtime")]
        if self.native_writer.take().is_some() {
            self.ended = true;
        }
        if !self.ended {
            if !self.opened {
                let _ = self.write_tx.send(
                    encode_open_stream_with_network(
                        self.session_id,
                        &self.target,
                        self.port,
                        self.network,
                        &[],
                    )
                    .unwrap_or_default(),
                );
            }
            let _ = self
                .write_tx
                .send(encode_end_stream(self.session_id).unwrap_or_default());
            self.ended = true;
        }
        if let Some(conn) = self.conn.take() {
            conn.release_stream(self.session_id);
        }
    }
}

impl AsyncRead for TrojanMuxStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(data) = &self.read_buf {
            let length = data.value().len();
            let n = (length - self.read_pos).min(buf.remaining());
            buf.put_slice(&data.value()[self.read_pos..self.read_pos + n]);
            self.read_pos += n;
            if self.read_pos == length {
                self.read_buf = None;
                self.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        match Pin::new(&mut self.read_rx).poll_recv(cx) {
            Poll::Ready(Some(TrojanMuxDownlink::Data(chunk))) => {
                let n = chunk.value().len().min(buf.remaining());
                buf.put_slice(&chunk.value()[..n]);
                if n < chunk.value().len() {
                    self.read_buf = Some(chunk);
                    self.read_pos = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(TrojanMuxDownlink::Overflow)) => {
                self.ended = true;
                Poll::Ready(Err(io::Error::other(
                    "Trojan MUX response backlog exceeded",
                )))
            }
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TrojanMuxStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        #[cfg(feature = "runtime")]
        if self.native_writer.is_some() {
            return self.poll_native_write(cx, buf);
        }
        if let Err(error) = self.flush_pending() {
            return Poll::Ready(Err(error));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        Poll::Ready(self.queue_frame(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        #[cfg(feature = "runtime")]
        if self.native_writer.is_some() {
            return self.poll_native_flush(cx);
        }
        Poll::Ready(self.flush_pending())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        #[cfg(feature = "runtime")]
        if self.native_writer.is_some() {
            return self.poll_native_shutdown(cx);
        }
        if let Err(error) = self.flush_pending() {
            return Poll::Ready(Err(error));
        }
        if !self.ended {
            if !self.opened {
                match encode_open_stream_with_network(
                    self.session_id,
                    &self.target,
                    self.port,
                    self.network,
                    &[],
                ) {
                    Ok(frame) => {
                        if self.write_tx.send(frame).is_err() {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "trojan mux writer closed",
                            )));
                        }
                        self.opened = true;
                    }
                    Err(error) => return Poll::Ready(Err(protocol_error(error))),
                }
            }
            match encode_end_stream(self.session_id) {
                Ok(frame) => {
                    if self.write_tx.send(frame).is_err() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "trojan mux writer closed",
                        )));
                    }
                }
                Err(error) => return Poll::Ready(Err(protocol_error(error))),
            }
            self.ended = true;
        }
        Poll::Ready(Ok(()))
    }
}

fn protocol_error(error: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(feature = "runtime")]
mod pending_end;
