//! User-space TCP termination stack.
//!
//! Implements [`TcpStack`] by maintaining a minimal TCP state machine
//! per connection.  Raw IP packets arrive via [`feed`]; the stack
//! completes three-way handshakes, extracts payload, and makes
//! established connections available via [`accept`].
//!
//! # State machine
//!
//! ```text
//!  SYN ──► SynReceived ──ACK──► Established ──FIN──► CloseWait ──FIN-ACK──► (removed)
//!                                         │
//!                                         └──proxy shutdown──► (FIN sent, removed)
//! ```
//!
//! [`feed`]: TcpStack::feed
//! [`accept`]: TcpStack::accept

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use zero_traits::{SocketAddress, TcpStack};

use crate::packet::{self, tcp_flags, Endpoint, ParsedTcp};

mod retransmission;
use retransmission::{RetransmissionResult, RetransmissionWait, TcpSendControl};

// ── ISS generator ─────────────────────────────────────────────────────

static NEXT_ISS: AtomicU32 = AtomicU32::new(1_000_000);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn next_iss() -> u32 {
    NEXT_ISS.fetch_add(128_000, Ordering::Relaxed)
}

fn sequence_before(candidate: u32, reference: u32) -> bool {
    (candidate.wrapping_sub(reference) as i32) < 0
}

fn sequence_after(candidate: u32, reference: u32) -> bool {
    sequence_before(reference, candidate)
}

// ── Connection key ────────────────────────────────────────────────────

/// (src_ip, src_port, dst_ip, dst_port) — as seen in the incoming packet.
type ConnKey = (IpAddr, u16, IpAddr, u16);

fn key_from_parsed(t: &ParsedTcp) -> ConnKey {
    (t.src.ip, t.src.port, t.dst.ip, t.dst.port)
}

fn key_reversed(k: &ConnKey) -> ConnKey {
    (k.2, k.3, k.0, k.1)
}

fn endpoint_to_sockaddr(ep: &Endpoint) -> SocketAddress {
    let ip = match ep.ip {
        IpAddr::V4(v4) => zero_traits::IpAddress::V4(v4.octets()),
        IpAddr::V6(v6) => zero_traits::IpAddress::V6(v6.octets()),
    };
    SocketAddress::new(ip, ep.port)
}

fn default_peer_mss(ip: IpAddr) -> u16 {
    if ip.is_ipv4() {
        536
    } else {
        1_220
    }
}

// ── TCP state ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpState {
    /// We sent SYN-ACK, waiting for ACK from client.
    SynReceived,
    /// Three-way handshake complete, data transfer.
    Established,
    /// Received FIN from client, waiting for our FIN-ACK to be sent.
    CloseWait,
}

/// Per-connection state tracked by the stack.
struct Conn {
    id: u64,
    state: TcpState,
    /// Next sequence number to send (our side).
    snd_nxt: Arc<AtomicU32>,
    /// Send-side acknowledgement and peer receive-window state.
    send_control: Arc<TcpSendControl>,
    /// Next expected receive sequence number (client side).
    rcv_nxt: Arc<AtomicU32>,
    /// Sends inbound payload toward the proxy (UserTcpStream read side).
    data_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// Read-side receiver — extracted when transitioning to Established
    /// and passed into the `UserTcpStream`.
    data_rx: Option<mpsc::Receiver<Vec<u8>>>,
    /// Last time we saw activity on this connection.
    last_active: Instant,
    /// Shared with the stream writer so the stack can retire a fully closed flow.
    fin_sent: Arc<AtomicBool>,
    peer_mss: u16,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.send_control.stop();
    }
}

fn accept_client_segment(conn: &mut Conn, tcp: &ParsedTcp<'_>) -> (bool, bool) {
    let mut needs_ack = false;
    let mut accepted_fin = false;
    let mut accepted_through = conn.rcv_nxt.load(Ordering::Acquire);

    if !tcp.payload.is_empty() {
        let Some(data_tx) = conn.data_tx.as_ref() else {
            return (false, false);
        };
        let payload_offset = if tcp.seq == accepted_through {
            Some(0)
        } else if sequence_before(tcp.seq, accepted_through) {
            let overlap = accepted_through.wrapping_sub(tcp.seq) as usize;
            (overlap < tcp.payload.len()).then_some(overlap)
        } else {
            None
        };

        if let Some(offset) = payload_offset {
            let payload = &tcp.payload[offset..];
            if data_tx.try_send(payload.to_vec()).is_ok() {
                accepted_through = accepted_through.wrapping_add(payload.len() as u32);
                conn.rcv_nxt.store(accepted_through, Ordering::Release);
            } else {
                // Keep rcv_nxt unchanged so the peer retransmits the segment.
                warn!("tcp conn data channel full, rejecting segment");
            }
        }
        needs_ack = true;
    }

    if tcp.fin {
        let fin_sequence = tcp.seq.wrapping_add(tcp.payload.len() as u32);
        if fin_sequence == accepted_through {
            accepted_through = accepted_through.wrapping_add(1);
            conn.rcv_nxt.store(accepted_through, Ordering::Release);
            // Dropping the last sender makes AsyncRead return EOF so the
            // kernel relay can finish its half-close normally.
            conn.data_tx.take();
            conn.state = TcpState::CloseWait;
            accepted_fin = true;
        }
        needs_ack = true;
    }

    (needs_ack, accepted_fin)
}

// ── UserTcpStream ─────────────────────────────────────────────────────

/// A TCP stream bridging the user-space stack to the proxy pipeline.
///
/// - `AsyncRead` returns data received from the application (via TUN).
/// - `AsyncWrite` wraps data in TCP segments and sends them through
///   the outbound packet channel (back to TUN).
pub struct UserTcpStream {
    /// Data from application (proxy reads this).
    read: StdMutex<TcpRead>,
    /// Connection metadata + outbound writer (proxy writes this).
    write: StdMutex<TcpWrite>,
}

struct TcpRead {
    receiver: mpsc::Receiver<Vec<u8>>,
    pending: Vec<u8>,
    pending_offset: usize,
    send_control: Arc<TcpSendControl>,
}

struct TcpWrite {
    /// Outbound packet channel (→ TUN writer task).
    outbound: mpsc::Sender<Vec<u8>>,
    /// Our IP (server side).
    src_ip: IpAddr,
    /// Application IP (client side).
    dst_ip: IpAddr,
    sport: u16,
    dport: u16,
    /// Next send sequence.
    snd_nxt: Arc<AtomicU32>,
    send_control: Arc<TcpSendControl>,
    /// Receive sequence (for ACK number).
    rcv_nxt: Arc<AtomicU32>,
    /// Have we sent FIN?
    fin_sent: Arc<AtomicBool>,
    mss: u16,
    outbound_reservation: Option<OutboundReservation>,
}

type OutboundReservation = Pin<
    Box<dyn Future<Output = Result<mpsc::OwnedPermit<Vec<u8>>, mpsc::error::SendError<()>>> + Send>,
>;

impl TcpWrite {
    fn new(
        outbound: mpsc::Sender<Vec<u8>>,
        conn_key: &ConnKey,
        snd_nxt: Arc<AtomicU32>,
        send_control: Arc<TcpSendControl>,
        rcv_nxt: Arc<AtomicU32>,
        fin_sent: Arc<AtomicBool>,
        mss: u16,
    ) -> Self {
        let rev = key_reversed(conn_key);
        Self {
            outbound,
            src_ip: rev.0,
            dst_ip: rev.2,
            sport: rev.1,
            dport: rev.3,
            snd_nxt,
            send_control,
            rcv_nxt,
            fin_sent,
            mss,
            outbound_reservation: None,
        }
    }

    fn poll_outbound_permit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<mpsc::OwnedPermit<Vec<u8>>>> {
        if self.outbound_reservation.is_none() {
            self.outbound_reservation = Some(Box::pin(self.outbound.clone().reserve_owned()));
        }
        let reservation = self
            .outbound_reservation
            .as_mut()
            .expect("TCP outbound reservation present");
        match reservation.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(permit)) => {
                self.outbound_reservation.take();
                Poll::Ready(Ok(permit))
            }
            Poll::Ready(Err(_)) => {
                self.outbound_reservation.take();
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "local TUN packet transport closed",
                )))
            }
        }
    }
}

impl UserTcpStream {
    fn new(data_rx: mpsc::Receiver<Vec<u8>>, write: TcpWrite) -> Self {
        let send_control = Arc::clone(&write.send_control);
        Self {
            read: StdMutex::new(TcpRead {
                receiver: data_rx,
                pending: Vec::new(),
                pending_offset: 0,
                send_control,
            }),
            write: StdMutex::new(write),
        }
    }
}

impl Drop for UserTcpStream {
    fn drop(&mut self) {
        let Ok(w) = self.write.lock() else {
            return;
        };
        if w.fin_sent.load(Ordering::Acquire) || w.send_control.io_error().is_some() {
            return;
        }
        let reset = packet::build_tcp(
            w.src_ip,
            w.dst_ip,
            w.sport,
            w.dport,
            w.snd_nxt.load(Ordering::Acquire),
            w.rcv_nxt.load(Ordering::Acquire),
            tcp_flags::RST | tcp_flags::ACK,
            &[],
        );
        if w.outbound.try_send(reset).is_ok() {
            w.fin_sent.store(true, Ordering::Release);
        }
        w.send_control.stop();
    }
}

impl AsyncRead for UserTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut read = self.read.lock().expect("TCP read lock poisoned");
        if let Some(error) = read.send_control.io_error() {
            return Poll::Ready(Err(error));
        }
        if read.pending_offset < read.pending.len() {
            let count = (read.pending.len() - read.pending_offset).min(buf.remaining());
            let end = read.pending_offset + count;
            buf.put_slice(&read.pending[read.pending_offset..end]);
            read.pending_offset = end;
            if read.pending_offset == read.pending.len() {
                read.pending.clear();
                read.pending_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        match read.receiver.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    read.pending = data;
                    read.pending_offset = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) if read.send_control.io_error().is_some() => Poll::Ready(Err(read
                .send_control
                .io_error()
                .expect("TCP error present"))),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => {
                read.send_control.register_reader(cx.waker());
                if let Some(error) = read.send_control.io_error() {
                    Poll::Ready(Err(error))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl AsyncWrite for UserTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut w = self.write.lock().expect("TCP write lock poisoned");
        if w.fin_sent.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection closed",
            )));
        }
        if let Some(error) = w.send_control.io_error() {
            return Poll::Ready(Err(error));
        }
        let snd_nxt = w.snd_nxt.load(Ordering::Acquire);
        let mut available = w.send_control.available_window(snd_nxt);
        if available == 0 {
            w.send_control.register_writer(cx.waker());
            available = w.send_control.available_window(snd_nxt);
            if available == 0 {
                return Poll::Pending;
            }
        }
        let count = data
            .len()
            .min(usize::from(w.mss.max(1)))
            .min(available as usize);
        let packet = packet::build_tcp(
            w.src_ip,
            w.dst_ip,
            w.sport,
            w.dport,
            snd_nxt,
            w.rcv_nxt.load(Ordering::Acquire),
            tcp_flags::PSH | tcp_flags::ACK,
            &data[..count],
        );
        let permit = match w.poll_outbound_permit(cx) {
            Poll::Ready(Ok(permit)) => permit,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        };
        let sequence_end = snd_nxt.wrapping_add(count as u32);
        w.snd_nxt.store(sequence_end, Ordering::Release);
        w.send_control.track_segment(sequence_end, packet.clone());
        permit.send(packet);
        Poll::Ready(Ok(count))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut w = self.write.lock().expect("TCP write lock poisoned");
        if w.fin_sent.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        if let Some(error) = w.send_control.io_error() {
            return Poll::Ready(Err(error));
        }
        let snd_nxt = w.snd_nxt.load(Ordering::Acquire);
        let mut available = w.send_control.available_window(snd_nxt);
        if available == 0 {
            w.send_control.register_writer(cx.waker());
            available = w.send_control.available_window(snd_nxt);
            if available == 0 {
                return Poll::Pending;
            }
        }
        let packet = packet::build_tcp(
            w.src_ip,
            w.dst_ip,
            w.sport,
            w.dport,
            snd_nxt,
            w.rcv_nxt.load(Ordering::Acquire),
            tcp_flags::FIN | tcp_flags::ACK,
            &[],
        );
        let permit = match w.poll_outbound_permit(cx) {
            Poll::Ready(Ok(permit)) => permit,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        };
        let sequence_end = snd_nxt.wrapping_add(1);
        w.snd_nxt.store(sequence_end, Ordering::Release);
        w.send_control.track_segment(sequence_end, packet.clone());
        permit.send(packet);
        w.fin_sent.store(true, Ordering::Release);
        Poll::Ready(Ok(()))
    }
}

// ── Established connection ────────────────────────────────────────────

struct ReadyConn {
    key: ConnKey,
    id: u64,
    stream: UserTcpStream,
    src: SocketAddress,
    dst: SocketAddress,
}

// ── UserTcpStack ──────────────────────────────────────────────────────

/// User-space TCP termination stack.
///
/// Implements [`TcpStack`].  Feed raw IP packets; accept established
/// connections.  Maintains a minimal per-connection TCP state machine
/// and emits response packets (SYN-ACK, ACK, FIN, RST) through an
/// internal outbound channel.
pub struct UserTcpStack {
    connections: Arc<Mutex<HashMap<ConnKey, Conn>>>,
    accept_tx: mpsc::Sender<ReadyConn>,
    accept_rx: Mutex<mpsc::Receiver<ReadyConn>>,
    outbound: mpsc::Sender<Vec<u8>>,
    mss: u16,
}

impl UserTcpStack {
    pub(crate) fn new(outbound: mpsc::Sender<Vec<u8>>, mss: u16) -> Self {
        let (tx, rx) = mpsc::channel::<ReadyConn>(64);
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            accept_tx: tx,
            accept_rx: Mutex::new(rx),
            outbound,
            mss,
        }
    }

    /// Send a response packet.  Silently drops if the channel is full.
    fn send_response(&self, pkt: Vec<u8>) {
        if let Err(e) = self.outbound.try_send(pkt) {
            warn!("tcp stack outbound full: {e}");
        }
    }

    /// Remove connections idle beyond `timeout`.
    pub async fn cleanup_idle(&self, timeout: std::time::Duration) {
        let mut conns = self.connections.lock().await;
        conns.retain(|_, conn| {
            let keep = conn.last_active.elapsed() < timeout;
            if !keep {
                conn.send_control.expire();
            }
            keep
        });
    }
}

async fn run_retransmission_worker(
    connections: std::sync::Weak<Mutex<HashMap<ConnKey, Conn>>>,
    key: ConnKey,
    connection_id: u64,
    send_control: Arc<TcpSendControl>,
    outbound: mpsc::WeakSender<Vec<u8>>,
) {
    loop {
        let notified = send_control.retransmission_notify.notified();
        match send_control.retransmission_wait(Instant::now()) {
            RetransmissionWait::Stopped => return,
            RetransmissionWait::Idle => notified.await,
            RetransmissionWait::Delay(delay) if delay.is_zero() => {}
            RetransmissionWait::Delay(delay) => {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = notified => {}
                }
            }
        }

        let Some(outbound) = outbound.upgrade() else {
            send_control.stop();
            return;
        };
        if matches!(
            send_control.retransmit_due(&outbound, Instant::now()),
            RetransmissionResult::Failed
        ) {
            send_control.wake_io();
            warn!(
                ?key,
                connection_id,
                error = ?send_control.io_error(),
                "user TCP retransmission failed"
            );
            if let Some(connections) = connections.upgrade() {
                let mut connections = connections.lock().await;
                if connections
                    .get(&key)
                    .is_some_and(|connection| connection.id == connection_id)
                {
                    connections.remove(&key);
                }
            }
            return;
        }
    }
}

impl TcpStack for UserTcpStack {
    type Connection = UserTcpStream;

    async fn feed(&self, packet: &[u8]) {
        if packet::ip_protocol(packet) != Some(packet::IPPROTO_TCP) {
            return;
        }
        let tcp = match packet::parse_tcp(packet) {
            Some(t) => t,
            None => return,
        };
        let tcp_window = packet::tcp_window(packet).unwrap_or(0);
        let advertised_mss = packet::tcp_mss(packet);
        let key = key_from_parsed(&tcp);
        let rev = key_reversed(&key);

        let mut conns = self.connections.lock().await;

        // ── RST: tear down immediately ──
        if tcp.rst {
            if let Some(conn) = conns.get(&key) {
                conn.send_control.observe_reset();
            }
            conns.remove(&key);
            return;
        }

        // ── Existing connection ──
        if let Some(conn) = conns.get_mut(&key) {
            conn.last_active = Instant::now();

            match conn.state {
                TcpState::SynReceived => {
                    if tcp.syn
                        && !tcp.ack_flag
                        && tcp.seq.wrapping_add(1) == conn.rcv_nxt.load(Ordering::Acquire)
                    {
                        conn.send_control.retry_now();
                        return;
                    }

                    let expected_ack = conn.snd_nxt.load(Ordering::Acquire);
                    let expected_sequence = conn.rcv_nxt.load(Ordering::Acquire);
                    if !tcp.ack_flag
                        || tcp.syn
                        || tcp.ack != expected_ack
                        || tcp.seq != expected_sequence
                    {
                        return;
                    }
                    conn.send_control
                        .observe_ack(tcp.ack, tcp_window, expected_ack);
                    conn.state = TcpState::Established;
                    tracing::trace!(?key, "user TCP handshake established");

                    // Extract the receiver that's been waiting since SYN.
                    let data_rx = conn.data_rx.take().expect("data_rx present in SynReceived");

                    let write = TcpWrite::new(
                        self.outbound.clone(),
                        &key,
                        Arc::clone(&conn.snd_nxt),
                        Arc::clone(&conn.send_control),
                        Arc::clone(&conn.rcv_nxt),
                        Arc::clone(&conn.fin_sent),
                        conn.peer_mss.min(self.mss),
                    );
                    let stream = UserTcpStream::new(data_rx, write);

                    let src = endpoint_to_sockaddr(&tcp.src);
                    let dst = endpoint_to_sockaddr(&tcp.dst);
                    if let Err(error) = self.accept_tx.try_send(ReadyConn {
                        key,
                        id: conn.id,
                        stream,
                        src,
                        dst,
                    }) {
                        warn!("tcp accept channel rejected connection: {error}");
                        conn.send_control.stop();
                        conns.remove(&key);
                        return;
                    }
                    tracing::trace!(?key, "user TCP connection queued for accept");

                    let (needs_ack, _) = accept_client_segment(conn, &tcp);
                    if needs_ack {
                        let ack = packet::build_tcp(
                            rev.0,
                            rev.2,
                            rev.1,
                            rev.3,
                            conn.snd_nxt.load(Ordering::Acquire),
                            conn.rcv_nxt.load(Ordering::Acquire),
                            tcp_flags::ACK,
                            &[],
                        );
                        self.send_response(ack);
                    }
                }
                TcpState::Established => {
                    if tcp.ack_flag {
                        conn.send_control.observe_ack(
                            tcp.ack,
                            tcp_window,
                            conn.snd_nxt.load(Ordering::Acquire),
                        );
                    }
                    let (needs_ack, accepted_fin) = accept_client_segment(conn, &tcp);

                    if needs_ack {
                        let ack = packet::build_tcp(
                            rev.0,
                            rev.2,
                            rev.1,
                            rev.3,
                            conn.snd_nxt.load(Ordering::Acquire),
                            conn.rcv_nxt.load(Ordering::Acquire),
                            tcp_flags::ACK,
                            &[],
                        );
                        self.send_response(ack);
                    }
                    if accepted_fin
                        && conn.fin_sent.load(Ordering::Acquire)
                        && tcp.ack_flag
                        && tcp.ack == conn.snd_nxt.load(Ordering::Acquire)
                    {
                        conn.send_control.stop();
                        conns.remove(&key);
                    }
                }
                TcpState::CloseWait => {
                    if tcp.ack_flag {
                        conn.send_control.observe_ack(
                            tcp.ack,
                            tcp_window,
                            conn.snd_nxt.load(Ordering::Acquire),
                        );
                    }
                    if tcp.ack_flag
                        && conn.fin_sent.load(Ordering::Acquire)
                        && tcp.ack == conn.snd_nxt.load(Ordering::Acquire)
                    {
                        conn.send_control.stop();
                        conns.remove(&key);
                        return;
                    }
                    // Waiting for proxy to finish.  ACK any retransmitted FINs.
                    if tcp.fin {
                        let fin_ack = packet::build_tcp(
                            rev.0,
                            rev.2,
                            rev.1,
                            rev.3,
                            conn.snd_nxt.load(Ordering::Acquire),
                            conn.rcv_nxt.load(Ordering::Acquire),
                            tcp_flags::ACK,
                            &[],
                        );
                        self.send_response(fin_ack);
                    }
                }
            }
            return;
        }

        // ── New connection: must be SYN ──
        if !tcp.syn {
            tracing::trace!(?key, "user TCP packet did not match an existing connection");
            return;
        }

        tracing::trace!(?key, "user TCP SYN created a connection");

        let iss = next_iss();
        let initial_rcv_nxt = tcp.seq.wrapping_add(1);
        let snd_nxt = Arc::new(AtomicU32::new(iss.wrapping_add(1)));
        let send_control = Arc::new(TcpSendControl::new(iss, tcp_window));
        let rcv_nxt = Arc::new(AtomicU32::new(initial_rcv_nxt));
        let fin_sent = Arc::new(AtomicBool::new(false));
        let peer_mss = advertised_mss
            .filter(|mss| *mss > 0)
            .unwrap_or_else(|| default_peer_mss(tcp.src.ip));

        // SYN-ACK with MSS option.
        let syn_ack = packet::build_tcp_with_mss(
            rev.0,
            rev.2,
            rev.1,
            rev.3,
            iss,
            initial_rcv_nxt,
            tcp_flags::SYN | tcp_flags::ACK,
            self.mss,
        );
        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(256);

        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        conns.insert(
            key,
            Conn {
                id: connection_id,
                state: TcpState::SynReceived,
                snd_nxt,
                send_control: Arc::clone(&send_control),
                rcv_nxt,
                data_tx: Some(data_tx),
                data_rx: Some(data_rx),
                last_active: Instant::now(),
                fin_sent,
                peer_mss,
            },
        );
        send_control.track_segment(iss.wrapping_add(1), syn_ack.clone());
        tokio::spawn(run_retransmission_worker(
            Arc::downgrade(&self.connections),
            key,
            connection_id,
            Arc::clone(&send_control),
            self.outbound.downgrade(),
        ));
        self.send_response(syn_ack);
    }

    async fn accept(&self) -> Option<(Self::Connection, SocketAddress, SocketAddress)> {
        let mut rx = self.accept_rx.lock().await;
        loop {
            let ready = rx.recv().await?;
            let is_current = self
                .connections
                .lock()
                .await
                .get(&ready.key)
                .is_some_and(|conn| {
                    conn.id == ready.id
                        && matches!(conn.state, TcpState::Established | TcpState::CloseWait)
                });
            if !is_current {
                tracing::debug!(
                    key = ?ready.key,
                    connection_id = ready.id,
                    "discarding stale user TCP accept entry"
                );
                continue;
            }
            tracing::trace!(
                source = ?ready.src,
                destination = ?ready.dst,
                connection_id = ready.id,
                "user TCP connection accepted"
            );
            return Some((ready.stream, ready.src, ready.dst));
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet;
    use std::net::Ipv4Addr;

    const CLIENT_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    const SERVER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    const CLIENT_PORT: u16 = 54321;
    const SERVER_PORT: u16 = 443;

    /// Helper: create a stack and drain outbound packets.
    fn new_stack() -> (UserTcpStack, mpsc::Receiver<Vec<u8>>) {
        let (out_tx, out_rx) = mpsc::channel(256);
        let stack = UserTcpStack::new(out_tx, 1500);
        (stack, out_rx)
    }

    /// Helper: drain all outbound packets and parse them as TCP.
    fn drain_outbound(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<ParsedTcp<'static>> {
        let mut results = Vec::new();
        while let Ok(pkt) = rx.try_recv() {
            if let Some(parsed) = packet::parse_tcp(&pkt) {
                // Safety: we only read the parsed fields, not the original buffer.
                // ParsedTcp borrows from the packet — extend its lifetime for tests.
                let owned = ParsedTcp {
                    src: parsed.src,
                    dst: parsed.dst,
                    seq: parsed.seq,
                    ack: parsed.ack,
                    syn: parsed.syn,
                    ack_flag: parsed.ack_flag,
                    fin: parsed.fin,
                    rst: parsed.rst,
                    psh: parsed.psh,
                    data_off: parsed.data_off,
                    payload: Vec::leak(parsed.payload.to_vec()),
                };
                results.push(owned);
            }
        }
        results
    }

    /// Helper: build a client → server TCP packet.
    fn client_packet(flags: u8, seq: u32, ack: u32, payload: &[u8]) -> Vec<u8> {
        if flags & tcp_flags::SYN != 0 && payload.is_empty() {
            return packet::build_tcp_with_mss(
                CLIENT_IP,
                SERVER_IP,
                CLIENT_PORT,
                SERVER_PORT,
                seq,
                ack,
                flags,
                1460,
            );
        }
        packet::build_tcp(
            CLIENT_IP,
            SERVER_IP,
            CLIENT_PORT,
            SERVER_PORT,
            seq,
            ack,
            flags,
            payload,
        )
    }

    #[tokio::test]
    async fn handshake_syn_ack_established() {
        let (stack, mut rx) = new_stack();

        // 1. Client sends SYN.
        let syn = client_packet(tcp_flags::SYN, 1000, 0, &[]);
        stack.feed(&syn).await;

        // Expect SYN-ACK.
        let out = drain_outbound(&mut rx);
        assert_eq!(out.len(), 1);
        assert!(out[0].syn);
        assert!(out[0].ack_flag);
        assert!(!out[0].fin);
        assert_eq!(out[0].src.port, SERVER_PORT);
        assert_eq!(out[0].dst.port, CLIENT_PORT);

        // 2. Client sends ACK to complete handshake.
        let server_seq = out[0].seq;
        let client_ack = server_seq.wrapping_add(1);
        let ack = client_packet(tcp_flags::ACK, 1001, client_ack, &[]);
        stack.feed(&ack).await;

        // Connection should be available via accept.
        let conn = tokio::time::timeout(std::time::Duration::from_millis(100), stack.accept())
            .await
            .unwrap();
        assert!(conn.is_some());
        let (_stream, src, _dst) = conn.unwrap();
        assert_eq!(src.port, CLIENT_PORT);
    }

    #[tokio::test]
    async fn data_transfer_bidirectional() {
        let (stack, mut rx) = new_stack();

        // Handshake.
        stack
            .feed(&client_packet(tcp_flags::SYN, 1000, 0, &[]))
            .await;
        let syn_ack = drain_outbound(&mut rx); // consume SYN-ACK
        let ack = client_packet(tcp_flags::ACK, 1001, syn_ack[0].seq + 1, &[]);
        stack.feed(&ack).await;

        // Accept the connection.
        let (stream, ..) = stack.accept().await.unwrap();

        // Client sends data.
        let data_pkt = client_packet(tcp_flags::PSH | tcp_flags::ACK, 1001, 0, b"hello");
        stack.feed(&data_pkt).await;

        // Read data from stream.
        use tokio::io::AsyncReadExt;
        let mut s = stream;
        let mut buf = [0u8; 32];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");

        // Verify ACK was sent back.
        let out = drain_outbound(&mut rx);
        assert!(out.iter().any(|p| p.ack_flag && !p.syn && !p.fin));
    }

    #[tokio::test]
    async fn fin_from_client_transitions_to_close_wait() {
        let (stack, mut rx) = new_stack();

        // Handshake.
        stack
            .feed(&client_packet(tcp_flags::SYN, 1000, 0, &[]))
            .await;
        let syn_ack = drain_outbound(&mut rx);
        stack
            .feed(&client_packet(
                tcp_flags::ACK,
                1001,
                syn_ack[0].seq + 1,
                &[],
            ))
            .await;
        let _stream = stack.accept().await;

        // Client sends FIN.
        let fin = client_packet(tcp_flags::FIN | tcp_flags::ACK, 1001, 0, &[]);
        stack.feed(&fin).await;

        // Expect ACK of the FIN.
        let out = drain_outbound(&mut rx);
        assert!(
            out.iter().any(|p| p.ack_flag && !p.syn && !p.fin),
            "should ACK the FIN"
        );

        // Connection should be in CloseWait, not removed.
        let conns = stack.connections.lock().await;
        let key = (CLIENT_IP, CLIENT_PORT, SERVER_IP, SERVER_PORT);
        let conn = conns
            .get(&key)
            .expect("connection should exist in CloseWait");
        assert_eq!(conn.state, TcpState::CloseWait);

        // Retransmitted FIN should get another ACK.
        drop(conns);
        stack.feed(&fin).await;
        let out2 = drain_outbound(&mut rx);
        assert!(
            out2.iter().any(|p| p.ack_flag && !p.syn && !p.fin),
            "should re-ACK retransmitted FIN"
        );
    }

    #[tokio::test]
    async fn rst_tears_down_immediately() {
        let (stack, mut rx) = new_stack();

        // Handshake.
        stack
            .feed(&client_packet(tcp_flags::SYN, 1000, 0, &[]))
            .await;
        let syn_ack = drain_outbound(&mut rx);
        stack
            .feed(&client_packet(
                tcp_flags::ACK,
                1001,
                syn_ack[0].seq + 1,
                &[],
            ))
            .await;
        let _stream = stack.accept().await;

        // Client sends RST.
        let rst = client_packet(tcp_flags::RST, 1001, 0, &[]);
        stack.feed(&rst).await;

        // No response should be sent for RST.
        let out = drain_outbound(&mut rx);
        assert!(out.is_empty(), "RST should not generate a response");

        // Connection should be gone.
        let conns = stack.connections.lock().await;
        let key = (CLIENT_IP, CLIENT_PORT, SERVER_IP, SERVER_PORT);
        assert!(conns.get(&key).is_none());
    }

    #[tokio::test]
    async fn non_syn_ignored_when_no_connection() {
        let (stack, mut rx) = new_stack();

        // Send ACK without prior SYN — should be silently dropped.
        let ack = client_packet(tcp_flags::ACK, 1000, 0, b"stray");
        stack.feed(&ack).await;

        let out = drain_outbound(&mut rx);
        assert!(out.is_empty());
    }
}
