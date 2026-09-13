//! Protocol-owned UDP pumps. Reading a partial frame survives concurrent sends.
use super::*;

#[cfg(test)]
#[path = "../../tests/udp/pump.rs"]
mod tests;

pub(super) fn spawn_udp_flow_task<S>(
    stream: S,
    mut send_rx: mpsc::Receiver<VlessUdpFlowSend>,
    responses: VlessUdpFlowResponses,
    flow_io: VlessEstablishedUdpFlow,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    tokio::spawn(async move {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let receive = async {
            let mut response_pending = true;
            while let Ok(Some(packet)) = flow_io
                .read_packet_tokio(&mut reader, &mut response_pending)
                .await
            {
                let _ = responses.send(packet.into_parts());
            }
        };
        let send = async {
            while let Some(request) = send_rx.recv().await {
                let (target, port, payload) = request.packet.into_parts();
                let result = flow_io
                    .write_packet_tokio(&mut writer, &target, port, &payload)
                    .await;
                let failed = result.is_err();
                let _ = request.result_tx.send(result);
                if failed {
                    break;
                }
            }
        };
        // Only connection termination cancels a partial read/write. A packet in
        // the opposite direction must never restart read_exact or block progress.
        tokio::select! { _ = receive => {}, _ = send => {} }
    });
}

pub(super) fn spawn_mux_udp_flow_task(
    mut send_rx: mpsc::Receiver<VlessUdpFlowSend>,
    up_tx: mpsc::UnboundedSender<zero_core::UdpFlowPacket>,
    mut down_rx: mpsc::Receiver<crate::mux_pool::MuxDownlink<zero_core::UdpFlowPacket>>,
    responses: VlessUdpFlowResponses,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                to_send = send_rx.recv() => {
                    match to_send {
                        Some(request) => {
                            let payload_len = request.packet.payload.len();
                            let result = up_tx
                                .send(request.packet)
                                .map(|_| payload_len)
                                .map_err(|_| Error::Io("vless mux udp flow closed"));
                            let should_break = result.is_err();
                            let _ = request.result_tx.send(result);
                            if should_break {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                read = down_rx.recv() => {
                    match read {
                        Some(crate::mux_pool::MuxDownlink::Data(packet)) => {
                            let packet = packet.into_inner();
                            let _ = responses.send(packet.into_parts());
                        }
                        Some(crate::mux_pool::MuxDownlink::Overflow) => break,
                        None => break,
                    }
                }
            }
        }
    });
}
