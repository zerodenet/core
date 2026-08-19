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
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use zero_traits::{SocketAddress, TcpStack};

use crate::packet::{self, tcp_flags, Endpoint, ParsedTcp};

// ── ISS generator ─────────────────────────────────────────────────────

static NEXT_ISS: AtomicU32 = AtomicU32::new(1_000_000);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn next_iss() -> u32 {
    NEXT_ISS.fetch_add(128_000, Ordering::Relaxed)
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
}

// ── UserTcpStream ─────────────────────────────────────────────────────

/// A TCP stream bridging the user-space stack to the proxy pipeline.
///
/// - `AsyncRead` returns data received from the application (via TUN).
/// - `AsyncWrite` wraps data in TCP segments and sends them through
///   the outbound packet channel (back to TUN).
pub struct UserTcpStream {
    /// Data from application (proxy reads this).
    read_rx: StdMutex<mpsc::Receiver<Vec<u8>>>,
    /// Connection metadata + outbound writer (proxy writes this).
    write: StdMutex<TcpWrite>,
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
    /// Receive sequence (for ACK number).
    rcv_nxt: Arc<AtomicU32>,
    /// Have we sent FIN?
    fin_sent: Arc<AtomicBool>,
}

impl UserTcpStream {
    fn new(
        data_rx: mpsc::Receiver<Vec<u8>>,
        outbound: mpsc::Sender<Vec<u8>>,
        conn_key: &ConnKey,
        snd_nxt: Arc<AtomicU32>,
        rcv_nxt: Arc<AtomicU32>,
        fin_sent: Arc<AtomicBool>,
    ) -> Self {
        // conn_key = (app_ip, app_port, our_ip, our_port)
        // We send FROM our side TO app side:
        let rev = key_reversed(conn_key);
        Self {
            read_rx: StdMutex::new(data_rx),
            write: StdMutex::new(TcpWrite {
                outbound,
                src_ip: rev.0, // our_ip
                dst_ip: rev.2, // app_ip
                sport: rev.1,  // our_port
                dport: rev.3,  // app_port
                snd_nxt,
                rcv_nxt,
                fin_sent,
            }),
        }
    }
}

impl AsyncRead for UserTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut rx = self.read_rx.lock().expect("TCP read lock poisoned");
        match rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UserTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let w = self.write.lock().expect("TCP write lock poisoned");
        if w.fin_sent.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection closed",
            )));
        }
        let pkt = packet::build_tcp(
            w.src_ip,
            w.dst_ip,
            w.sport,
            w.dport,
            w.snd_nxt.load(Ordering::Acquire),
            w.rcv_nxt.load(Ordering::Acquire),
            tcp_flags::PSH | tcp_flags::ACK,
            data,
        );
        match w.outbound.try_send(pkt) {
            Ok(()) => {
                w.snd_nxt.fetch_add(data.len() as u32, Ordering::AcqRel);
                Poll::Ready(Ok(data.len()))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let w = self.write.lock().expect("TCP write lock poisoned");
        if w.fin_sent.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        let pkt = packet::build_tcp(
            w.src_ip,
            w.dst_ip,
            w.sport,
            w.dport,
            w.snd_nxt.load(Ordering::Acquire),
            w.rcv_nxt.load(Ordering::Acquire),
            tcp_flags::FIN | tcp_flags::ACK,
            &[],
        );
        match w.outbound.try_send(pkt) {
            Ok(()) => {
                w.snd_nxt.fetch_add(1, Ordering::AcqRel);
                w.fin_sent.store(true, Ordering::Release);
                Poll::Ready(Ok(()))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")))
            }
        }
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
    connections: Mutex<HashMap<ConnKey, Conn>>,
    accept_tx: mpsc::Sender<ReadyConn>,
    accept_rx: Mutex<mpsc::Receiver<ReadyConn>>,
    outbound: mpsc::Sender<Vec<u8>>,
    mss: u16,
}

impl UserTcpStack {
    pub(crate) fn new(outbound: mpsc::Sender<Vec<u8>>, mss: u16) -> Self {
        let (tx, rx) = mpsc::channel::<ReadyConn>(64);
        Self {
            connections: Mutex::new(HashMap::new()),
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
        conns.retain(|_, conn| conn.last_active.elapsed() < timeout);
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
        let key = key_from_parsed(&tcp);
        let rev = key_reversed(&key);

        let mut conns = self.connections.lock().await;

        // ── RST: tear down immediately ──
        if tcp.rst {
            conns.remove(&key);
            return;
        }

        // ── Existing connection ──
        if let Some(conn) = conns.get_mut(&key) {
            conn.last_active = Instant::now();

            match conn.state {
                TcpState::SynReceived => {
                    // Expect ACK completing the handshake.
                    if !tcp.ack_flag || tcp.syn {
                        return;
                    }
                    conn.state = TcpState::Established;
                    tracing::trace!(?key, "user TCP handshake established");

                    // Extract the receiver that's been waiting since SYN.
                    let data_rx = conn.data_rx.take().expect("data_rx present in SynReceived");

                    let stream = UserTcpStream::new(
                        data_rx,
                        self.outbound.clone(),
                        &key,
                        Arc::clone(&conn.snd_nxt),
                        Arc::clone(&conn.rcv_nxt),
                        Arc::clone(&conn.fin_sent),
                    );

                    let src = endpoint_to_sockaddr(&tcp.src);
                    let dst = endpoint_to_sockaddr(&tcp.dst);
                    match self.accept_tx.try_send(ReadyConn {
                        key,
                        id: conn.id,
                        stream,
                        src,
                        dst,
                    }) {
                        Ok(()) => tracing::trace!(?key, "user TCP connection queued for accept"),
                        Err(error) => {
                            warn!("tcp accept channel rejected connection: {error}");
                            conns.remove(&key);
                        }
                    }
                }
                TcpState::Established => {
                    let mut needs_ack = false;

                    // Data transfer. Pure ACKs do not elicit another ACK.
                    if !tcp.payload.is_empty() {
                        let Some(data_tx) = conn.data_tx.as_ref() else {
                            return;
                        };
                        if data_tx.try_send(tcp.payload.to_vec()).is_err() {
                            // Channel full — proxy is slow.  Drop the packet;
                            // the client will retransmit.
                            warn!("tcp conn data channel full, dropping segment");
                            return;
                        }
                        conn.rcv_nxt
                            .fetch_add(tcp.payload.len() as u32, Ordering::AcqRel);
                        needs_ack = true;
                    }

                    // FIN from client → transition to CloseWait.
                    if tcp.fin {
                        conn.rcv_nxt.fetch_add(1, Ordering::AcqRel);
                        // Dropping the last sender makes AsyncRead return EOF so the
                        // kernel relay can finish its half-close normally.
                        conn.data_tx.take();
                        conn.state = TcpState::CloseWait;
                        needs_ack = true;
                    }

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
                TcpState::CloseWait => {
                    if tcp.ack_flag
                        && conn.fin_sent.load(Ordering::Acquire)
                        && tcp.ack == conn.snd_nxt.load(Ordering::Acquire)
                    {
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
        let rcv_nxt = Arc::new(AtomicU32::new(initial_rcv_nxt));
        let fin_sent = Arc::new(AtomicBool::new(false));

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

        conns.insert(
            key,
            Conn {
                id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
                state: TcpState::SynReceived,
                snd_nxt,
                rcv_nxt,
                data_tx: Some(data_tx),
                data_rx: Some(data_rx),
                last_active: Instant::now(),
                fin_sent,
            },
        );

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
                .is_some_and(|conn| conn.id == ready.id && conn.state == TcpState::Established);
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
        drain_outbound(&mut rx); // consume SYN-ACK
        let ack = client_packet(tcp_flags::ACK, 1001, 0, &[]);
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
        drain_outbound(&mut rx);
        stack
            .feed(&client_packet(tcp_flags::ACK, 1001, 0, &[]))
            .await;
        let _ = stack.accept().await;

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
        drain_outbound(&mut rx);
        stack
            .feed(&client_packet(tcp_flags::ACK, 1001, 0, &[]))
            .await;
        let _ = stack.accept().await;

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
