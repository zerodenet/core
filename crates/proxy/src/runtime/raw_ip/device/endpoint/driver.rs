//! Original endpoint driver; owned buffers remain live through its actual send.
use super::*;

pub(super) async fn run_endpoint_stack(stack: EndpointStack) -> Result<(), EngineError> {
    let EndpointStack {
        peer,
        mut endpoint,
        tcp,
        udp,
        mut raw_packets,
        mut forwarded_packets,
        closed,
        returns,
    } = stack;
    let mut sweep = tokio::time::interval(Duration::from_secs(5));
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = sweep.tick() => {
                returns.expire();
                udp.cleanup_expired(std::time::Instant::now());
            },
            packet = raw_packets.recv() => {
                let Some(zero_stack::packet_output::ObservedPacket { packet, observer }) = packet else { break; };
                let packets = if packet::ip_protocol(&packet) == Some(packet::IPPROTO_TCP) {
                    tcp.fragment_outbound_packet_buffer(packet).await.map_err(std::io::Error::other)?
                } else { vec![packet] };
                for packet in packets {
                    if !send_endpoint_packet(&mut endpoint, EndpointPacket { peer, packet: packet.into(), observer: observer.clone(), closed: closed.clone(), return_channel: None }).await {
                        closed.store(true, Ordering::Release);
                        return Ok(());
                    }
                }
            }
            packets = forwarded_packets.recv() => {
                let Some(ForwardedPackets { packets, observer, return_channel }) = packets else { break; };
                for packet in packets {
                    if return_channel.is_closed() { if let Some(observer) = &observer { observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed); } continue; }
                    if !send_endpoint_packet(&mut endpoint, EndpointPacket { peer, packet, observer: observer.clone(), closed: closed.clone(), return_channel: Some(return_channel.clone()) }).await {
                        closed.store(true, Ordering::Release);
                        return Ok(());
                    }
                }
            }
        }
    }
    closed.store(true, Ordering::Release);
    Ok(())
}

async fn send_endpoint_packet(
    endpoint: &mut watch::Receiver<mpsc::Sender<EndpointPacket>>,
    mut packet: EndpointPacket,
) -> bool {
    loop {
        let sender = endpoint.borrow().clone();
        match sender.send(packet).await {
            Ok(()) => return true,
            Err(error) => packet = error.0,
        }
        if endpoint.changed().await.is_err() {
            return false;
        }
    }
}
