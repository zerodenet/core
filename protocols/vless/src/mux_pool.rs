//! VLESS MUX connection pool shared types.
//!
//! Types that are pure VLESS MUX protocol logic live here.
//! Connection establishment (raw TCP, transport wrapping) stays in the
//! proxy crate which owns the I/O infrastructure.

use core::future::Future;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, watch};

type OpenedUdpStream = (
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
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    server: String,
    port: u16,
    identity: MuxIdentity,
    transport: TransportKey,
    idle_timeout: Option<Duration>,
    response_backlog: MuxResponseBacklogPolicy,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct MuxIdentity {
    uuid: [u8; 16],
}

#[derive(Clone, PartialEq, Eq, Hash)]
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

#[derive(Clone)]
pub struct MuxConnectionPool {
    pool: Arc<Mutex<HashMap<PoolKey, Arc<MuxPoolConn>>>>,
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
        Self { uuid }
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
        establish_outbound_mux_connection(stream, self.uuid()).await
    }

    fn into_pool_conn<S>(self, stream: S, max_concurrency: u32) -> MuxPoolConn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        MuxPoolConn::new(
            stream,
            self.uuid(),
            max_concurrency,
            self.idle_timeout,
            self.response_backlog,
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
        }
    }

    pub fn evict_all(&self) {
        self.pool.lock().expect("mux pool lock poisoned").clear();
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
    {
        let cached = {
            let pool = self.pool.lock().expect("mux pool lock poisoned");
            pool.get(&key).cloned()
        };

        if let Some(conn) = cached {
            if let Some(sid) = conn.try_reserve_stream_id() {
                return Ok((conn, sid));
            }
        }

        let conn = Arc::new(create_conn(key.clone(), max_concurrency).await?);
        let sid = conn
            .try_reserve_stream_id()
            .expect("new VLESS MUX connection accepts its first stream");
        self.pool
            .lock()
            .expect("mux pool lock poisoned")
            .insert(key, conn.clone());
        Ok((conn, sid))
    }
}

// ── Pool connection ──

/// A single MUX connection to an upstream, shared by multiple streams.
struct MuxPoolConn {
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>>,
    next_id: Mutex<u16>,
    active: Mutex<usize>,
    max_concurrency: u32,
    closed: Arc<AtomicBool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
    response_backlog_frames: usize,
}

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
    fn new<S>(
        stream: S,
        uuid: &[u8; 16],
        max_concurrency: u32,
        idle_timeout: Option<Duration>,
        response_backlog_policy: MuxResponseBacklogPolicy,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(stream);
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let response_backlog = MuxResponseBacklog::from_policy(response_backlog_policy);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let activity_tx = idle_timeout.map(|idle_timeout| {
            let (activity_tx, activity_rx) = watch::channel(tokio::time::Instant::now());
            spawn_mux_idle_monitor(
                idle_timeout,
                activity_rx,
                closed.clone(),
                shutdown_tx.clone(),
            );
            activity_tx
        });
        let _ = uuid;

        spawn_mux_write_relay(
            write_half,
            write_rx,
            closed.clone(),
            shutdown_tx.clone(),
            shutdown_rx.clone(),
            activity_tx.clone(),
        );
        spawn_mux_read_relay(
            read_half,
            streams.clone(),
            closed.clone(),
            shutdown_tx.clone(),
            shutdown_rx,
            activity_tx.clone(),
            response_backlog.clone(),
        );

        Self {
            write_tx,
            streams,
            next_id: Mutex::new(1),
            active: Mutex::new(0),
            max_concurrency,
            closed,
            activity_tx,
            response_backlog_frames: response_backlog_policy.frames(),
        }
    }

    fn open_tcp_stream(
        self: &Arc<Self>,
        sid: u16,
        port: u16,
        address: &Address,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Unpin + 'static, Error> {
        let (up_tx, up_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (down_tx, down_rx) = mpsc::channel(self.response_backlog_frames + 1);

        self.streams.lock().unwrap().insert(
            sid,
            MuxClientStreamState {
                downlink: MuxClientDownlink::Tcp(down_tx),
                target: Some((address.clone(), port)),
            },
        );

        let req = encode_mux_new_stream(sid, crate::mux::NETWORK_TCP, port, address)?;
        if self.write_tx.send(req).is_err() {
            self.release_stream(sid);
            return Err(Error::Io("failed to write VLESS MUX new stream request"));
        }

        spawn_mux_tcp_upload_relay(self.clone(), sid, up_rx);

        Ok(MuxStreamRelay {
            up_tx,
            sid,
            down_rx: Some(down_rx),
            read_pending: Vec::new(),
            read_offset: 0,
            conn: self.clone(),
        })
    }

    fn open_udp_stream(
        self: &Arc<Self>,
        sid: u16,
        global_id: [u8; 8],
    ) -> Result<OpenedUdpStream, Error> {
        let (up_tx, up_rx) = mpsc::unbounded_channel::<UdpFlowPacket>();
        let (down_tx, down_rx) = mpsc::channel(self.response_backlog_frames + 1);

        self.streams.lock().unwrap().insert(
            sid,
            MuxClientStreamState {
                downlink: MuxClientDownlink::Udp(down_tx),
                target: None,
            },
        );
        spawn_mux_udp_upload_relay(self.clone(), sid, global_id, up_rx);

        Ok((sid, up_tx, down_rx))
    }

    fn try_reserve_stream_id(&self) -> Option<u16> {
        let streams = self.streams.lock().unwrap();
        let mut active = self.active.lock().unwrap();
        if self.closed.load(Ordering::Acquire) || *active >= self.max_concurrency as usize {
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
}

// ── MUX stream relay ──

/// A single MUX stream — implements `AsyncRead` + `AsyncWrite` over the
/// shared MUX connection.
struct MuxStreamRelay {
    up_tx: mpsc::UnboundedSender<Vec<u8>>,
    sid: u16,
    down_rx: Option<mpsc::Receiver<MuxDownlink<Vec<u8>>>>,
    read_pending: Vec<u8>,
    read_offset: usize,
    conn: Arc<MuxPoolConn>,
}

impl Drop for MuxStreamRelay {
    fn drop(&mut self) {
        self.conn.release_stream(self.sid);
    }
}

impl AsyncRead for MuxStreamRelay {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.read_offset < self.read_pending.len() {
            let remaining = &self.read_pending[self.read_offset..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.read_offset += n;
            if self.read_offset == self.read_pending.len() {
                self.read_pending.clear();
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
                let data = data.into_inner();
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.read_pending = data;
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
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.up_tx
            .send(buf.to_vec())
            .map(|_| Poll::Ready(Ok(buf.len())))
            .unwrap_or_else(|_| {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "MUX upstream closed",
                )))
            })
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

// ── Mux.Cool stream relays ──

fn spawn_mux_tcp_upload_relay(
    conn: Arc<MuxPoolConn>,
    sid: u16,
    mut up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let write = conn.write_tx.clone();
    tokio::spawn(async move {
        while let Some(payload) = up_rx.recv().await {
            let Ok(frame) = encode_mux_data_frame(sid, &payload) else {
                break;
            };
            if write.send(frame).is_err() {
                break;
            }
        }
        if let Ok(close_frame) = encode_mux_end_frame(sid) {
            let _ = write.send(close_frame);
        }
    });
}

fn spawn_mux_udp_upload_relay(
    conn: Arc<MuxPoolConn>,
    sid: u16,
    global_id: [u8; 8],
    mut up_rx: mpsc::UnboundedReceiver<UdpFlowPacket>,
) {
    let write = conn.write_tx.clone();
    let streams = conn.streams.clone();
    tokio::spawn(async move {
        let mut first = true;
        while let Some(packet) = up_rx.recv().await {
            let (target, port, payload) = packet.into_parts();
            if let Some(state) = streams.lock().unwrap().get_mut(&sid) {
                state.target = Some((target.clone(), port));
            }
            let frame = if first {
                first = false;
                crate::mux::encode_new_udp_data_frame(sid, &target, port, global_id, &payload)
            } else {
                crate::mux::encode_udp_data_frame(sid, &target, port, &payload)
            };
            let Ok(frame) = frame else {
                break;
            };
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
    });
}

fn spawn_mux_write_relay<W>(
    mut writer: W,
    mut write_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    closed: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                frame = write_rx.recv() => {
                    let Some(frame) = frame else {
                        break;
                    };
                    if writer.write_all(&frame).await.is_err() {
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
    });
}

fn spawn_mux_read_relay<R>(
    mut reader: R,
    streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>>,
    closed: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
    activity_tx: Option<watch::Sender<tokio::time::Instant>>,
    response_backlog: MuxResponseBacklog,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    closed.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(true);
                    streams.lock().unwrap().clear();
                    return;
                }
            }
            response = crate::udp::read_vless_response_tokio(&mut reader) => {
                if response.is_err() {
                    closed.store(true, Ordering::Release);
                    let _ = shutdown_tx.send(true);
                    streams.lock().unwrap().clear();
                    return;
                }
            }
        }
        loop {
            let frame = tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
                frame = crate::mux::read_mux_frame_tokio(&mut reader) => {
                    match frame {
                        Ok(frame) => frame,
                        Err(_) => break,
                    }
                }
            };
            if let Some(activity_tx) = &activity_tx {
                activity_tx.send_replace(tokio::time::Instant::now());
            }
            if frame.session_id == 0 {
                continue;
            }
            if frame.status == crate::mux::STATUS_END {
                streams.lock().unwrap().remove(&frame.session_id);
                continue;
            }
            if frame.options & crate::mux::OPTION_DATA == 0 {
                continue;
            }

            let session_id = frame.session_id;
            let mut streams = streams.lock().unwrap();
            let Some(state) = streams.get_mut(&session_id) else {
                continue;
            };
            let queued = match &state.downlink {
                MuxClientDownlink::Tcp(tx) => {
                    let payload_len = frame.payload.len();
                    try_queue_mux_response(&response_backlog, tx, frame.payload, payload_len)
                }
                MuxClientDownlink::Udp(tx) => {
                    if let Some(target) = frame.target {
                        state.target = Some((target.address, target.port));
                    }
                    if let Some((target, port)) = &state.target {
                        let payload_len = frame.payload.len();
                        try_queue_mux_response(
                            &response_backlog,
                            tx,
                            UdpFlowPacket::new(target.clone(), *port, frame.payload),
                            payload_len,
                        )
                    } else {
                        true
                    }
                }
            };
            if !queued {
                streams.remove(&session_id);
            }
        }
        closed.store(true, Ordering::Release);
        let _ = shutdown_tx.send(true);
        streams.lock().unwrap().clear();
    });
}

fn try_queue_mux_response<T>(
    backlog: &MuxResponseBacklog,
    tx: &mpsc::Sender<MuxDownlink<T>>,
    value: T,
    bytes: usize,
) -> bool {
    if tx.capacity() <= 1 {
        let _ = tx.try_send(MuxDownlink::Overflow);
        return false;
    }
    let Ok(response) = backlog.try_buffer(bytes, value) else {
        let _ = tx.try_send(MuxDownlink::Overflow);
        return false;
    };
    if tx.try_send(MuxDownlink::Data(response)).is_err() {
        let _ = tx.try_send(MuxDownlink::Overflow);
        return false;
    }
    true
}

fn spawn_mux_idle_monitor(
    idle_timeout: Duration,
    mut activity_rx: watch::Receiver<tokio::time::Instant>,
    closed: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        loop {
            let deadline = *activity_rx.borrow() + idle_timeout;
            tokio::select! {
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
    });
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
