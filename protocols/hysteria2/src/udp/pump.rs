//! Coupled UDP task lifetime, including connections with QUIC keep-alive enabled.
use super::*;

#[cfg(feature = "runtime")]
pub fn spawn_udp_flow(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    initial_packet: Hysteria2InitialUdpFlowPacket,
    flow_io: Hysteria2UdpFlowIo,
) -> Hysteria2UdpFlowHandle {
    let (send_tx, send_rx) = mpsc::channel::<UdpFlowPacket>(32);
    let (responses, _) = broadcast::channel::<Hysteria2UdpFlowResponse>(32);

    let response_tx = responses.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = send_packets(conn.clone(), initial_packet, flow_io, send_rx) => {},
            _ = receive_packets(conn, flow_io, response_tx) => {},
        }
    });

    Hysteria2UdpFlowHandle {
        sender: Hysteria2UdpFlowSender { send_tx },
        responses,
    }
}

#[cfg(feature = "runtime")]
pub fn start_udp_flow_with_initial_packet(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    target: &Address,
    port: u16,
    payload: &[u8],
    resume: Hysteria2UdpFlowResume,
) -> Hysteria2UdpFlowConnection {
    let flow_io = resume.flow_io();
    let initial_packet = Hysteria2InitialUdpFlowPacket::from_parts(target, port, payload);
    Hysteria2UdpFlowConnection::new(Hysteria2UdpFlowSession::new(spawn_udp_flow(
        conn,
        initial_packet,
        flow_io,
    )))
}

#[cfg(feature = "runtime")]
async fn send_packets(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    initial_packet: Hysteria2InitialUdpFlowPacket,
    flow_io: Hysteria2UdpFlowIo,
    mut send_rx: mpsc::Receiver<UdpFlowPacket>,
) {
    let Some(max_datagram_size) = conn.connection().max_datagram_size() else {
        return;
    };
    let Ok(fragments) = flow_io.encode_fragments(&initial_packet.packet, max_datagram_size) else {
        return;
    };
    for fragment in fragments {
        if conn.connection().send_datagram(fragment.into()).is_err() {
            return;
        }
    }
    while let Some(packet) = send_rx.recv().await {
        let Ok(fragments) = flow_io.encode_fragments(&packet, max_datagram_size) else {
            break;
        };
        for fragment in fragments {
            if conn.connection().send_datagram(fragment.into()).is_err() {
                return;
            }
        }
    }
}

#[cfg(feature = "runtime")]
async fn receive_packets(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    flow_io: Hysteria2UdpFlowIo,
    responses: Hysteria2UdpFlowResponses,
) {
    let mut reassembler = Hysteria2UdpReassembler::default();
    while let Ok(data) = conn.connection().read_datagram().await {
        let Ok(fragment) = parse_udp_datagram(&data) else {
            continue;
        };
        if fragment.session_id != flow_io.session_id {
            continue;
        }
        let Ok(Some(packet)) = reassembler.push(fragment) else {
            continue;
        };
        let (_, _, target, port, payload) = packet.into_parts();
        if responses.send((target, port, payload)).is_err() {
            break;
        }
    }
}
