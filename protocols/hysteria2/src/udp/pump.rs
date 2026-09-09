//! Logical UDP flow lifetime; the connection owns the single datagram reader.
use super::*;

pub fn start_udp_flow_with_initial_packet(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    target: &Address,
    port: u16,
    payload: &[u8],
    _resume: Hysteria2UdpFlowResume,
) -> Result<Hysteria2UdpFlowConnection, Error> {
    spawn(
        conn,
        Hysteria2InitialUdpFlowPacket::from_parts(target, port, payload),
        None,
    )
}

pub(crate) fn start_managed_udp_flow(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    target: &Address,
    port: u16,
    payload: &[u8],
    lifetime: tokio::sync::watch::Receiver<()>,
) -> Result<Hysteria2UdpFlowConnection, Error> {
    spawn(
        conn,
        Hysteria2InitialUdpFlowPacket::from_parts(target, port, payload),
        Some(lifetime),
    )
}

fn spawn(
    conn: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    initial_packet: Hysteria2InitialUdpFlowPacket,
    mut lifetime: Option<tokio::sync::watch::Receiver<()>>,
) -> Result<Hysteria2UdpFlowConnection, Error> {
    let closed = conn.connection().clone();
    let channel = Hysteria2UdpChannel::new(conn)?;
    let (send_tx, mut send_rx) = mpsc::channel::<UdpFlowPacket>(32);
    let (responses, _) = broadcast::channel::<Hysteria2UdpFlowResponse>(32);
    let response_tx = responses.clone();
    let response_ready = Arc::new(tokio::sync::Notify::new());
    let ready = response_ready.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = closed.closed() => {},
            _ = async {
                if let Some(lifetime) = &mut lifetime { let _ = lifetime.changed().await; }
                else { std::future::pending::<()>().await; }
            } => {},
            _ = async {
                let packet = initial_packet.packet;
                channel.send_to(&packet.target, packet.port, &packet.payload).await?;
                while let Some(packet) = send_rx.recv().await {
                    channel.send_to(&packet.target, packet.port, &packet.payload).await?;
                }
                Ok::<_, Error>(())
            } => {},
            _ = async {
                while let Ok(mut packet) = channel.receive().await {
                    loop {
                        match response_tx.send(packet) {
                            Ok(_) => break,
                            Err(unsent) => { packet = unsent.0; ready.notified().await; }
                        }
                    }
                }
            } => {},
        }
        // Dropping the registration closes only this session, not sibling TCP/UDP users.
    });
    Ok(Hysteria2UdpFlowConnection::new(
        Hysteria2UdpFlowSession::new(Hysteria2UdpFlowHandle {
            response_ready,
            sender: Hysteria2UdpFlowSender { send_tx },
            responses,
        }),
    ))
}
