//! VLESS MUX connection pool shared types.
//!
//! Types that are pure VLESS MUX protocol logic live here.
//! Connection establishment (raw TCP, transport wrapping) stays in the
//! proxy crate which owns the I/O infrastructure.
pub(crate) mod native;

mod response;
use response::run_mux_read_relay;

use core::future::Future;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, watch};

pub(crate) type OpenedUdpStream = (
    u16,
    mpsc::UnboundedSender<zero_core::UdpFlowPacket>,
    mpsc::Receiver<MuxDownlink<zero_core::UdpFlowPacket>>,
);
use zero_core::{Address, Error, UdpFlowPacket};

use crate::mux::backlog::{BufferedMuxResponse, MuxResponseBacklog, MuxResponseBacklogPolicy};

#[cfg(test)]
mod tests;

// ── Pool key types ──

/// Identifies a unique upstream endpoint including transport.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    server: String,
    port: u16,
    identity: MuxIdentity,
    transport: TransportKey,
    idle_timeout: Option<Duration>,
    response_backlog: MuxResponseBacklogPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MuxIdentity {
    uuid: [u8; 16],
    vision: bool,
    vision_testseed: [u32; 4],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TransportKey {
    Raw,
    Tls {
        server_name: Option<String>,
    },
    Reality {
        public_key: String,
        server_name: String,
        client_fingerprint: String,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct MuxTransportProfile<'a> {
    tls_server_name: Option<&'a str>,
    reality_public_key: Option<&'a str>,
    reality_server_name: Option<&'a str>,
    reality_client_fingerprint: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnedMuxTransportProfile {
    tls_server_name: Option<String>,
    reality_public_key: Option<String>,
    reality_server_name: Option<String>,
    reality_client_fingerprint: Option<String>,
}

type NativeBinding = zero_platform_tokio::contract::network::UpstreamBinding;
type PoolEntries = HashMap<(Option<Arc<str>>, PoolKey, Option<NativeBinding>), Arc<MuxPoolConn>>;

#[derive(Clone)]
pub struct MuxConnectionPool {
    control: zero_transport::pooled_driver::PoolControl,
    pool: Arc<Mutex<PoolEntries>>,
    scope: Option<Arc<str>>,
}

struct PoolKeyConfig {
    server: String,
    port: u16,
    identity: MuxIdentity,
    tls_server_name: Option<String>,
    reality_public_key: Option<String>,
    reality_server_name: Option<String>,
    reality_client_fingerprint: Option<String>,
    idle_timeout: Option<Duration>,
    response_backlog: MuxResponseBacklogPolicy,
}

impl PoolKeyConfig {
    fn new(server: impl Into<String>, port: u16, identity: MuxIdentity) -> Self {
        Self {
            server: server.into(),
            port,
            identity,
            tls_server_name: None,
            reality_public_key: None,
            reality_server_name: None,
            reality_client_fingerprint: None,
            idle_timeout: None,
            response_backlog: MuxResponseBacklogPolicy::default(),
        }
    }

    fn with_tls_server_name(mut self, server_name: Option<&str>) -> Self {
        self.tls_server_name = server_name.map(ToOwned::to_owned);
        self
    }

    fn with_reality(
        mut self,
        public_key: Option<&str>,
        server_name: Option<&str>,
        client_fingerprint: Option<&str>,
    ) -> Self {
        self.reality_public_key = public_key.map(ToOwned::to_owned);
        self.reality_server_name = server_name.map(ToOwned::to_owned);
        self.reality_client_fingerprint = client_fingerprint.map(ToOwned::to_owned);
        self
    }

    fn with_idle_timeout_secs(mut self, idle_timeout_secs: Option<u64>) -> Self {
        self.idle_timeout = idle_timeout_secs.map(Duration::from_secs);
        self
    }

    fn with_response_backlog(mut self, response_backlog: MuxResponseBacklogPolicy) -> Self {
        self.response_backlog = response_backlog;
        self
    }

    fn into_pool_key(self) -> PoolKey {
        PoolKey::from_config_parts(
            self.server,
            self.port,
            self.identity,
            self.tls_server_name.as_deref(),
            self.reality_public_key.as_deref(),
            self.reality_server_name.as_deref(),
            self.reality_client_fingerprint.as_deref(),
            self.idle_timeout,
            self.response_backlog,
        )
    }
}

fn transport_key_from_config(
    tls_server_name: Option<&str>,
    reality_public_key: Option<&str>,
    reality_server_name: Option<&str>,
    reality_client_fingerprint: Option<&str>,
    fallback_server: &str,
) -> TransportKey {
    match (
        tls_server_name,
        reality_public_key,
        reality_server_name,
        reality_client_fingerprint,
    ) {
        (Some(server_name), None, _, _) => TransportKey::Tls {
            server_name: Some(server_name.to_owned()),
        },
        (None, Some(public_key), server_name, client_fingerprint) => TransportKey::Reality {
            public_key: public_key.to_owned(),
            server_name: server_name.unwrap_or(fallback_server).to_owned(),
            client_fingerprint: client_fingerprint.unwrap_or("chrome").to_owned(),
        },
        _ => TransportKey::Raw,
    }
}

pub(crate) fn pool_key_from_transport_config(
    server: &str,
    port: u16,
    identity: MuxIdentity,
    profile: MuxTransportProfile<'_>,
    idle_timeout_secs: Option<u64>,
    response_backlog: MuxResponseBacklogPolicy,
) -> PoolKey {
    PoolKeyConfig::new(server, port, identity)
        .with_tls_server_name(profile.tls_server_name)
        .with_reality(
            profile.reality_public_key,
            profile.reality_server_name,
            profile.reality_client_fingerprint,
        )
        .with_idle_timeout_secs(idle_timeout_secs)
        .with_response_backlog(response_backlog)
        .into_pool_key()
}

impl<'a> MuxTransportProfile<'a> {
    pub const fn new(
        tls_server_name: Option<&'a str>,
        reality_public_key: Option<&'a str>,
        reality_server_name: Option<&'a str>,
        reality_client_fingerprint: Option<&'a str>,
    ) -> Self {
        Self {
            tls_server_name,
            reality_public_key,
            reality_server_name,
            reality_client_fingerprint,
        }
    }
}

impl OwnedMuxTransportProfile {
    pub(crate) fn new(
        tls_server_name: Option<String>,
        reality_public_key: Option<String>,
        reality_server_name: Option<String>,
        reality_client_fingerprint: Option<String>,
    ) -> Self {
        Self {
            tls_server_name,
            reality_public_key,
            reality_server_name,
            reality_client_fingerprint,
        }
    }

    pub(crate) fn as_borrowed(&self) -> MuxTransportProfile<'_> {
        MuxTransportProfile::new(
            self.tls_server_name.as_deref(),
            self.reality_public_key.as_deref(),
            self.reality_server_name.as_deref(),
            self.reality_client_fingerprint.as_deref(),
        )
    }
}

impl MuxIdentity {
    pub(crate) fn from_uuid(uuid: [u8; 16]) -> Self {
        Self {
            uuid,
            vision: false,
            vision_testseed: crate::validation::DEFAULT_VISION_TESTSEED,
        }
    }

    pub(crate) fn with_vision_testseed(mut self, vision: bool, testseed: [u32; 4]) -> Self {
        self.vision = vision;
        self.vision_testseed = testseed;
        self
    }

    fn uuid(&self) -> &[u8; 16] {
        &self.uuid
    }
}

impl PoolKey {
    fn from_identity(
        server: String,
        port: u16,
        identity: MuxIdentity,
        transport: TransportKey,
        idle_timeout: Option<Duration>,
        response_backlog: MuxResponseBacklogPolicy,
    ) -> Self {
        Self {
            server,
            port,
            identity,
            transport,
            idle_timeout,
            response_backlog,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn from_config_parts(
        server: String,
        port: u16,
        identity: MuxIdentity,
        tls_server_name: Option<&str>,
        reality_public_key: Option<&str>,
        reality_server_name: Option<&str>,
        reality_client_fingerprint: Option<&str>,
        idle_timeout: Option<Duration>,
        response_backlog: MuxResponseBacklogPolicy,
    ) -> Self {
        let transport = transport_key_from_config(
            tls_server_name,
            reality_public_key,
            reality_server_name,
            reality_client_fingerprint,
            &server,
        );
        Self::from_identity(
            server,
            port,
            identity,
            transport,
            idle_timeout,
            response_backlog,
        )
    }

    fn uuid(&self) -> &[u8; 16] {
        self.identity.uuid()
    }

    async fn establish_mux_connection<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: zero_traits::AsyncSocket,
    {
        if self.identity.vision {
            if stream.transport_bypass_control().is_none() {
                return Err(Error::Unsupported(
                    "Vision requires TLS 1.3, REALITY or VLESS Encryption",
                ));
            }
            let mut request = vec![crate::shared::VLESS_VERSION];
            request.extend_from_slice(self.uuid());
            request.extend(crate::flow::encode_addons(Some(
                crate::flow::FLOW_XTLS_RPRX_VISION,
            ))?);
            request.push(3);
            stream
                .write_all(&request)
                .await
                .map_err(|_| Error::Io("failed to write Vision XUDP request"))
        } else {
            establish_outbound_mux_connection(stream, self.uuid()).await
        }
    }

    fn into_pool_conn<S>(self, stream: S, max_concurrency: u32) -> MuxPoolConn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if self.identity.vision {
            return MuxPoolConn::new(
                crate::vision::VisionStream::with_testseed(
                    crate::deferred_response::DeferredVlessResponseStream::new(stream),
                    *self.uuid(),
                    None,
                    self.identity.vision_testseed,
                ),
                self.uuid(),
                max_concurrency,
                self.idle_timeout,
                self.response_backlog,
                false,
            );
        }
        MuxPoolConn::new(
            stream,
            self.uuid(),
            max_concurrency,
            self.idle_timeout,
            self.response_backlog,
            true,
        )
    }
    fn prepare_pool_conn<S>(
        self,
        stream: S,
        max_concurrency: u32,
    ) -> (MuxPoolConn, zero_transport::pooled_driver::Driver)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if self.identity.vision {
            return MuxPoolConn::prepare(
                crate::vision::VisionStream::with_testseed(
                    crate::deferred_response::DeferredVlessResponseStream::new(stream),
                    *self.uuid(),
                    None,
                    self.identity.vision_testseed,
                ),
                self.uuid(),
                max_concurrency,
                self.idle_timeout,
                self.response_backlog,
                false,
            );
        }
        MuxPoolConn::prepare(
            stream,
            self.uuid(),
            max_concurrency,
            self.idle_timeout,
            self.response_backlog,
            true,
        )
    }
}

pub async fn establish_outbound_mux_connection<S>(
    stream: &mut S,
    id: &[u8; 16],
) -> Result<(), Error>
where
    S: zero_traits::AsyncSocket,
{
    crate::outbound::establish_outbound_mux_connection(stream, id).await
}

impl Default for MuxConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for MuxConnectionPool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MuxConnectionPool")
            .field(
                "entries",
                &self.pool.lock().expect("mux pool lock poisoned").len(),
            )
            .finish()
    }
}

impl MuxConnectionPool {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(Mutex::new(HashMap::new())),
            scope: None,
            control: Default::default(),
        }
    }

    /// A cold view of the same actual adapter pool. Scope is private protocol
    /// state, never a new pool/worker or a native resource-close receipt.
    pub(crate) fn scoped(&self, tag: &str, transport: impl std::fmt::Debug) -> Self {
        let scope: Arc<str> = format!("{tag:?}:{transport:?}").into();
        Self {
            pool: self.pool.clone(),
            // Ownership is stable for the configured node across transport
            // changes; the actual cache scope still includes the full profile.
            control: self.control.scoped(Arc::from(tag)),
            scope: Some(scope),
        }
    }

    pub fn evict_all(&self) {
        self.pool
            .lock()
            .expect("mux pool lock poisoned")
            .retain(|_, connection| connection.original.is_some());
    }

    pub(crate) async fn open_tcp_stream<S, OpenStream, OpenStreamFut, E>(
        &self,
        key: PoolKey,
        max_concurrency: u32,
        port: u16,
        address: &Address,
        open_stream: OpenStream,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Unpin + 'static, E>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.control
            .enter_legacy()
            .map_err(|_| E::from(Error::Io("MUX pool has an installed native owner")))?;
        let (conn, sid) = self
            .get_or_create_conn(key, max_concurrency, |key, max_concurrency| async move {
                let mut stream = match open_stream().await {
                    Ok(stream) => stream,
                    Err(error) => return Err(error),
                };
                if let Err(error) = key.establish_mux_connection(&mut stream).await {
                    return Err(E::from(error));
                }
                Ok(key.into_pool_conn(stream, max_concurrency))
            })
            .await?;
        conn.open_tcp_stream(sid, port, address).map_err(E::from)
    }

    pub(crate) async fn open_udp_stream<S, OpenStream, OpenStreamFut, E>(
        &self,
        key: PoolKey,
        max_concurrency: u32,
        global_id: [u8; 8],
        open_stream: OpenStream,
    ) -> Result<OpenedUdpStream, E>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.control
            .enter_legacy()
            .map_err(|_| E::from(Error::Io("MUX pool has an installed native owner")))?;
        let (conn, sid) = self
            .get_or_create_conn(key, max_concurrency, |key, max_concurrency| async move {
                let mut stream = match open_stream().await {
                    Ok(stream) => stream,
                    Err(error) => return Err(error),
                };
                if let Err(error) = key.establish_mux_connection(&mut stream).await {
                    return Err(E::from(error));
                }
                Ok(key.into_pool_conn(stream, max_concurrency))
            })
            .await?;
        conn.open_udp_stream(sid, global_id).map_err(E::from)
    }

    async fn get_or_create_conn<F, Fut, E>(
        &self,
        key: PoolKey,
        max_concurrency: u32,
        create_conn: F,
    ) -> Result<(Arc<MuxPoolConn>, u16), E>
    where
        F: FnOnce(PoolKey, u32) -> Fut,
        Fut: Future<Output = Result<MuxPoolConn, E>>,
        E: From<Error>,
    {
        self.get_or_create_conn_with_binding(key, max_concurrency, None, create_conn)
            .await
    }
    async fn get_or_create_conn_with_binding<F, Fut, E>(
        &self,
        key: PoolKey,
        max_concurrency: u32,
        binding: Option<NativeBinding>,
        create_conn: F,
    ) -> Result<(Arc<MuxPoolConn>, u16), E>
    where
        F: FnOnce(PoolKey, u32) -> Fut,
        Fut: Future<Output = Result<MuxPoolConn, E>>,
        E: From<Error>,
    {
        {
            let mut after = None;
            loop {
                let connection = {
                    let entries = self.pool.lock().expect("actual protocol MUX map");
                    entries
                        .values()
                        .filter(|actual| {
                            after.is_none_or(|after| Arc::as_ptr(actual) as usize > after)
                        })
                        .min_by_key(|actual| Arc::as_ptr(actual) as usize)
                        .cloned()
                };
                let Some(connection) = connection else { break };
                after = Some(Arc::as_ptr(&connection) as usize);
                // Actual child Root/receiver observation and any temporary
                // final header destruction remain outside the protocol map.
                if !connection
                    .original
                    .as_ref()
                    .is_some_and(|original| original.confirmed_closed())
                {
                    continue;
                }
                let removed = {
                    let mut entries = self.pool.lock().expect("actual protocol MUX map");
                    entries
                        .extract_if(|_, actual| Arc::ptr_eq(actual, &connection))
                        .next()
                };
                drop(removed);
            }
        }
        let lookup = (self.scope.clone(), key.clone(), binding);
        let cached = {
            let pool = self.pool.lock().expect("mux pool lock poisoned");
            pool.get(&lookup).cloned()
        };

        if let Some(conn) = cached {
            if let Some(sid) = conn.try_reserve_stream_id() {
                return Ok((conn, sid));
            }
        }

        let conn = Arc::new(create_conn(key.clone(), max_concurrency).await?);
        let sid = conn
            .try_reserve_stream_id()
            .ok_or_else(|| E::from(Error::Io("MUX connection retired during preparation")))?;
        self.pool
            .lock()
            .expect("mux pool lock poisoned")
            .insert(lookup, conn.clone());
        Ok((conn, sid))
    }
}

// ── Pool connection ──

/// A single MUX connection to an upstream, shared by multiple streams.
pub(crate) struct MuxPoolConn {
    pub(crate) inbound_caller: Option<zero_platform_tokio::AcceptedCaller>,
    original: Option<Arc<zero_transport::pooled_driver::Physical>>,
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    native_udp_tx: zero_transport::pooled_driver::datagram_queue::PacketQueue,
    streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>>,
    next_id: Mutex<u16>,
    active: Mutex<usize>,
    max_concurrency: u32,
    closed: Arc<AtomicBool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
    response_backlog_frames: usize,
    total: AtomicU32,
    shutdown: watch::Sender<bool>,
}

#[derive(Clone)]
enum MuxClientDownlink {
    Tcp(mpsc::Sender<MuxDownlink<Vec<u8>>>),
    Udp(mpsc::Sender<MuxDownlink<UdpFlowPacket>>),
}

pub(crate) enum MuxDownlink<T> {
    Data(BufferedMuxResponse<T>),
    Overflow,
}

struct MuxClientStreamState {
    downlink: MuxClientDownlink,
    target: Option<(Address, u16)>,
}

impl MuxPoolConn {
    pub(crate) fn new<S>(
        stream: S,
        uuid: &[u8; 16],
        max_concurrency: u32,
        idle_timeout: Option<Duration>,
        response_backlog_policy: MuxResponseBacklogPolicy,
        response_pending: bool,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, driver) = Self::prepare(
            stream,
            uuid,
            max_concurrency,
            idle_timeout,
            response_backlog_policy,
            response_pending,
        );
        tokio::spawn(driver);
        connection
    }

    pub(crate) fn prepare<S>(
        stream: S,
        uuid: &[u8; 16],
        max_concurrency: u32,
        idle_timeout: Option<Duration>,
        response_backlog_policy: MuxResponseBacklogPolicy,
        response_pending: bool,
    ) -> (Self, zero_transport::pooled_driver::Driver)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(stream);
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (native_udp_tx, native_udp_rx) =
            zero_transport::pooled_driver::datagram_queue::channel_with_limits(
                8,
                (max_concurrency as usize).max(1).min(u16::MAX as usize),
                65535 + 1024,
            )
            .expect("bounded actual MUX FIFO limits");
        let streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>> =
            Arc::new(Mutex::new(HashMap::new()));
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
        let write = run_mux_write_relay(
            write_half,
            write_rx,
            native_udp_rx,
            closed.clone(),
            shutdown_tx.clone(),
            shutdown_rx.clone(),
            activity_tx.clone(),
        );
        let read = run_mux_read_relay(
            read_half,
            streams.clone(),
            closed.clone(),
            (shutdown_tx.clone(), shutdown_rx),
            activity_tx.clone(),
            response_backlog,
            response_pending,
        );
        let stopping = closed.clone();
        let stop = shutdown_tx.clone();
        let outputs = streams.clone();
        let driver = zero_transport::pooled_driver::join(read, write, idle, move || {
            stopping.store(true, Ordering::Release);
            let _ = stop.send(true);
            outputs.lock().unwrap().clear();
        });
        let _ = uuid;

        (
            Self {
                inbound_caller: None,
                original: None,
                write_tx,
                native_udp_tx,
                streams,
                next_id: Mutex::new(1),
                active: Mutex::new(0),
                max_concurrency,
                closed,
                activity_tx,
                response_backlog_frames: response_backlog_policy.frames(),
                total: AtomicU32::new(0),
                shutdown: shutdown_tx,
            },
            driver,
        )
    }

    pub(crate) fn open_tcp_stream(
        self: &Arc<Self>,
        sid: u16,
        port: u16,
        address: &Address,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Unpin + 'static, Error> {
        self.open_tcp_stream_with_origin(sid, port, address, None)
    }
    pub(crate) fn open_tcp_stream_with_origin(
        self: &Arc<Self>,
        sid: u16,
        port: u16,
        address: &Address,
        origin: Option<&crate::mux::origin::Origin>,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Unpin + 'static, Error> {
        let (down_tx, down_rx) = mpsc::channel(self.response_backlog_frames);

        self.streams.lock().unwrap().insert(
            sid,
            MuxClientStreamState {
                downlink: MuxClientDownlink::Tcp(down_tx),
                target: Some((address.clone(), port)),
            },
        );

        let mut req = encode_mux_new_stream(sid, crate::mux::NETWORK_TCP, port, address)?;
        if let Some(origin) = origin {
            origin.append(&mut req)?;
        }
        if self.write_tx.send(req).is_err() {
            self.release_stream(sid);
            return Err(Error::Io("failed to write VLESS MUX new stream request"));
        }

        Ok(MuxStreamRelay {
            native_writer: None,
            native_initial: None,
            write_open: true,
            sid,
            down_rx: Some(down_rx),
            read_pending: None,
            read_offset: 0,
            conn: self.clone(),
        })
    }

    pub(crate) fn open_udp_stream(
        self: &Arc<Self>,
        sid: u16,
        global_id: [u8; 8],
    ) -> Result<OpenedUdpStream, Error> {
        self.open_udp_stream_with_id(sid, Some(global_id))
    }

    pub(crate) fn open_udp_stream_with_id(
        self: &Arc<Self>,
        sid: u16,
        global_id: Option<[u8; 8]>,
    ) -> Result<OpenedUdpStream, Error> {
        self.open_udp_stream_with_origin(sid, global_id, None)
    }
    pub(crate) fn open_udp_stream_with_origin(
        self: &Arc<Self>,
        sid: u16,
        global_id: Option<[u8; 8]>,
        origin: Option<crate::mux::origin::Origin>,
    ) -> Result<OpenedUdpStream, Error> {
        let (up_tx, up_rx) = mpsc::unbounded_channel::<UdpFlowPacket>();
        let (down_tx, down_rx) = mpsc::channel(self.response_backlog_frames);

        self.streams.lock().unwrap().insert(
            sid,
            MuxClientStreamState {
                downlink: MuxClientDownlink::Udp(down_tx),
                target: None,
            },
        );
        if let Err(error) = spawn_mux_udp_upload_relay(self.clone(), sid, global_id, origin, up_rx)
        {
            self.release_stream(sid);
            return Err(error);
        }

        Ok((sid, up_tx, down_rx))
    }

    pub(crate) fn try_reserve_stream_id(&self) -> Option<u16> {
        let streams = self.streams.lock().unwrap();
        let mut active = self.active.lock().unwrap();
        if self.closed.load(Ordering::Acquire)
            || *active >= self.max_concurrency.min(u16::MAX as u32) as usize
        {
            return None;
        }
        let sid = loop {
            let mut next = self.next_id.lock().unwrap();
            let s = *next;
            *next = next.wrapping_add(1);
            if *next == 0 {
                *next = 1;
            }
            drop(next);
            if !streams.contains_key(&s) {
                break s;
            }
        };
        *active += 1;
        self.total.fetch_add(1, Ordering::Relaxed);
        self.touch_idle();
        Some(sid)
    }

    fn release_stream(self: &Arc<Self>, sid: u16) {
        self.streams.lock().unwrap().remove(&sid);
        let mut active = self.active.lock().unwrap();
        *active = active.saturating_sub(1);
    }

    fn touch_idle(&self) {
        if let Some(activity_tx) = &self.activity_tx {
            activity_tx.send_replace(tokio::time::Instant::now());
        }
    }

    pub(crate) fn active_connections(&self) -> usize {
        *self.active.lock().unwrap()
    }
    pub(crate) fn total_connections(&self) -> u32 {
        self.total.load(Ordering::Relaxed)
    }
    pub(crate) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.shutdown.send_replace(true);
    }
    pub(crate) async fn wait_closed(&self) {
        let mut shutdown = self.shutdown.subscribe();
        while !*shutdown.borrow_and_update() && !self.closed() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    }
}

// ── MUX stream relay ──

/// A single MUX stream — implements `AsyncRead` + `AsyncWrite` over the
/// shared MUX connection.
struct MuxStreamRelay {
    native_writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    native_initial: Option<(u16, Address)>,
    write_open: bool,
    sid: u16,
    down_rx: Option<mpsc::Receiver<MuxDownlink<Vec<u8>>>>,
    read_pending: Option<BufferedMuxResponse<Vec<u8>>>,
    read_offset: usize,
    conn: Arc<MuxPoolConn>,
}

impl MuxStreamRelay {
    fn end_upload(&mut self) {
        if self.write_open {
            self.write_open = false;
            if self.native_writer.take().is_some() {
                return;
            }
            if let Ok(end) = encode_mux_end_frame(self.sid) {
                let _ = self.conn.write_tx.send(end);
            }
        }
    }
}

impl Drop for MuxStreamRelay {
    fn drop(&mut self) {
        self.end_upload();
        self.conn.release_stream(self.sid);
    }
}

impl AsyncRead for MuxStreamRelay {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        std::task::ready!(self.poll_native_initial(cx))?;
        if let Some(data) = &self.read_pending {
            let remaining = &data.value()[self.read_offset..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            let length = data.value().len();
            self.read_offset += n;
            if self.read_offset == length {
                self.read_pending = None;
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }
        let rx = match &mut self.down_rx {
            Some(rx) => rx,
            None => return Poll::Ready(Ok(())),
        };
        match rx.poll_recv(cx) {
            Poll::Ready(Some(MuxDownlink::Data(data))) => {
                let n = data.value().len().min(buf.remaining());
                buf.put_slice(&data.value()[..n]);
                if n < data.value().len() {
                    self.read_pending = Some(data);
                    self.read_offset = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(MuxDownlink::Overflow)) => {
                self.down_rx = None;
                Poll::Ready(Err(io::Error::other("VLESS MUX response backlog exceeded")))
            }
            Poll::Ready(None) => {
                self.down_rx = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for MuxStreamRelay {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        std::task::ready!(self.poll_native_initial(cx))?;
        let count = buf.len().min(crate::mux::MUX_MAX_PAYLOAD);
        if count == 0 {
            return Poll::Ready(Ok(0));
        }
        if !self.write_open || self.conn.closed() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "MUX upload is closed",
            )));
        }
        if let Some(writer) = &mut self.native_writer {
            std::task::ready!(writer.poll_ready(cx))?;
        }
        let frame = match encode_mux_data_frame(self.sid, &buf[..count]) {
            Ok(frame) => frame,
            Err(source) => return Poll::Ready(Err(io::Error::other(source))),
        };
        if let Some(writer) = &mut self.native_writer {
            return Poll::Ready(writer.send(frame).map(|_| count));
        }
        Poll::Ready(
            self.conn
                .write_tx
                .send(frame)
                .map(|_| count)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "MUX upstream closed")),
        )
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.poll_native_initial(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        std::task::ready!(self.poll_native_initial(cx))?;
        self.end_upload();
        Poll::Ready(Ok(()))
    }
}

// ── Mux.Cool stream relays ──

fn spawn_mux_udp_upload_relay(
    conn: Arc<MuxPoolConn>,
    sid: u16,
    global_id: Option<[u8; 8]>,
    origin: Option<crate::mux::origin::Origin>,
    mut up_rx: mpsc::UnboundedReceiver<UdpFlowPacket>,
) -> Result<(), Error> {
    let write = conn.write_tx.clone();
    let streams = conn.streams.clone();
    let caller = conn.inbound_caller.clone();
    let driver = async move {
        let mut first = true;
        while let Some(packet) = up_rx.recv().await {
            let (target, port, payload) = packet.into_parts();
            if let Some(state) = streams.lock().unwrap().get_mut(&sid) {
                state.target = Some((target.clone(), port));
            }
            let initial = first;
            let frame = if first {
                first = false;
                match global_id {
                    Some(global_id) => crate::mux::encode_new_udp_data_frame(
                        sid, &target, port, global_id, &payload,
                    ),
                    None => crate::mux::codec::encode_new_udp_data_frame_optional(
                        sid, &target, port, None, &payload,
                    ),
                }
            } else {
                crate::mux::encode_udp_data_frame(sid, &target, port, &payload)
            };
            let Ok(mut frame) = frame else {
                break;
            };
            if initial {
                if let Some(origin) = &origin {
                    if origin.append(&mut frame).is_err() {
                        break;
                    }
                }
            }
            if write.send(frame).is_err() {
                break;
            }
        }
        if let Ok(close_frame) = encode_mux_end_frame(sid) {
            let _ = write.send(close_frame);
        }
        streams.lock().unwrap().remove(&sid);
        let mut active = conn.active.lock().unwrap();
        *active = active.saturating_sub(1);
    };
    if let Some(caller) = caller {
        caller
            .spawn(
                zero_platform_tokio::contract::scope::TaskClass::Auxiliary,
                move |_| async move {
                    driver.await;
                    Ok(())
                },
            )
            .map_err(|_| Error::Io("Rvs original UDP driver registration failed"))?;
    } else {
        tokio::spawn(driver);
    }
    Ok(())
}

async fn run_mux_write_relay<W>(
    mut writer: W,
    mut write_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    mut native_udp_rx: mpsc::Receiver<zero_transport::pooled_driver::datagram_queue::QueuedPacket>,
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
                tokio::select! {
                    frame=write_rx.recv()=>frame,
                    frame=native_udp_rx.recv()=>frame.map(|frame|frame.into_bytes()),
                }
            } => {
                let Some(frame) = frame else {
                    break;
                };
                if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
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

fn encode_mux_new_stream(
    session_id: u16,
    network: u8,
    port: u16,
    address: &Address,
) -> Result<Vec<u8>, Error> {
    crate::mux::encode_new_stream(session_id, network, port, address)
}

fn encode_mux_data_frame(session_id: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
    crate::mux::encode_data_frame(session_id, payload)
}

fn encode_mux_end_frame(session_id: u16) -> Result<Vec<u8>, Error> {
    crate::mux::encode_end_frame(session_id)
}
