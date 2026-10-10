//! Atomic client reply classification and delivery under one connection lock.

use super::*;

impl ClientTcpStack {
    /// Return true for a matching tuple even when its TCP state rejects the
    /// segment. Such replies must never become fresh inbound connections.
    pub async fn feed_correlated(&self, raw_packet: &[u8]) -> bool {
        let Some(tcp) = packet::parse_tcp(raw_packet) else {
            return false;
        };
        let key = key_from_parsed(&tcp);
        let rev = key_reversed(&key);
        let mut connections = self.connections.lock().await;
        let Some(conn) = connections.get_mut(&key) else {
            return false;
        };
        self.control_packets.ensure_worker();
        if let Some(observer) = &conn.observer {
            observer.received(raw_packet.len());
        }
        conn.last_active = Instant::now();
        if tcp.rst {
            conn.send_control.observe_reset();
            connections.remove(&key);
            return true;
        }
        let peer_window = packet::tcp_window(raw_packet).unwrap_or(0);
        match conn.state {
            TcpState::SynSent => {
                let expected_ack = conn.snd_nxt.load(Ordering::Acquire);
                if !tcp.syn || !tcp.ack_flag || tcp.ack != expected_ack {
                    return true;
                }
                conn.rcv_nxt
                    .store(tcp.seq.wrapping_add(1), Ordering::Release);
                conn.peer_mss = packet::tcp_mss(raw_packet)
                    .filter(|mss| *mss > 0)
                    .unwrap_or_else(|| default_peer_mss(tcp.src.ip));
                conn.send_control
                    .observe_ack(tcp.ack, peer_window, expected_ack);
                conn.state = TcpState::Established;
                let write = TcpWrite::new(
                    self.outbound.clone(),
                    self.control_packets.clone(),
                    &key,
                    conn,
                    conn.peer_mss.min(self.mss),
                );
                let stream = UserTcpStream::new(Arc::clone(&conn.receive_buffer), write);
                let ack = packet::build_tcp_with_window(
                    rev.0,
                    rev.2,
                    rev.1,
                    rev.3,
                    expected_ack,
                    conn.rcv_nxt.load(Ordering::Acquire),
                    tcp_flags::ACK,
                    conn.receive_buffer.window(),
                    &[],
                );
                self.control_packets
                    .clone()
                    .with_observer(conn.observer.clone())
                    .try_send(ack);
                if let Some(waiter) = conn.connect_waiter.take() {
                    let _ = waiter.send(stream);
                }
            }
            TcpState::Established | TcpState::CloseWait => {
                if tcp.ack_flag {
                    conn.send_control.observe_ack(
                        tcp.ack,
                        peer_window,
                        conn.snd_nxt.load(Ordering::Acquire),
                    );
                }
                if tcp.syn {
                    self.send_ack(conn, rev);
                    return true;
                }
                let (needs_ack, accepted_fin) = accept_client_segment(conn, &tcp);
                if needs_ack {
                    self.send_ack(conn, rev);
                }
                if accepted_fin
                    && conn.fin_sent.load(Ordering::Acquire)
                    && tcp.ack_flag
                    && tcp.ack == conn.snd_nxt.load(Ordering::Acquire)
                {
                    conn.send_control.stop();
                    connections.remove(&key);
                }
            }
            TcpState::SynReceived => {}
        }
        true
    }
}
