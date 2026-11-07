//! Same standalone wire body and exact original Stack, no receiving task.
use super::*;
use crate::runtime::raw_ip::PreparedWireReceive;
pub(super) async fn finish(
    device: &mut Device,
    received: PreparedWireReceive<(Vec<RawIpAction>, bool)>,
    sender: Option<std::net::SocketAddr>,
    read: &NativeReadGrant,
) -> Result<Option<(Vec<RawIpAction>, bool)>, EngineError> {
    let mut data = match received {
        PreparedWireReceive::Complete(received) => return Ok(Some(received)),
        PreparedWireReceive::Data(data) => data,
    };
    // Preserve original roaming-family and AllowedIPs checks before handing
    // anything to a matched application's actual Stack consumer.
    let source_matches = sender.is_none_or(|sender| sender.is_ipv4() == device.endpoint.is_ipv4());
    if data.allowed && source_matches {
        let prepared = device
            .tcp
            .prepare_correlated_owned(&data.packet, read)
            .await
            .map_err(std::io::Error::other)?;
        if let Some(prepared) = prepared {
            if let Some(routes) = device.returns.fence_tcp_delivery(&data.packet) {
                if let Err(error) = device.tunnel.commit_prepared_datagram(&data) {
                    tracing::debug!(
                        ?error,
                        "prepared wire Data lost actual Session/replay admission"
                    );
                    return Ok(None);
                }
                drop(routes);
                device
                    .traffic
                    .rx(TrafficPlane::Inner, data.packet.len(), true);

                device
                    .health
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .last_authenticated_packet = Some(Instant::now());
                assert!(
                    prepared.consume(),
                    "same actual tuple stayed locked through commit"
                );
                return Ok(Some((Vec::new(), true)));
            }
        }
        if let Err(error) = data.admit_forward() {
            let failure = data.rejected(error);
            if matches!(
                error,
                zero_traits::PacketDeriveError::Busy | zero_traits::PacketDeriveError::Capacity
            ) {
                device.traffic.dropped_reason(
                    TrafficPlane::Outer,
                    false,
                    zero_api::TrafficDropReason::QueueFull,
                );
                tracing::debug!(source=?failure.source,"prepared wire Data was not submitted: transmit capacity");
                return Ok(None);
            }
            return Err(failure.source);
        }
    }
    if let Err(error) = device.tunnel.commit_prepared_datagram(&data) {
        tracing::debug!(
            ?error,
            "prepared wire Data lost actual Session/replay admission"
        );
        return Ok(None);
    }
    if let Some(source) = packet::ip_source(&data.packet) {
        Ok(Some((
            vec![RawIpAction::ReceiveIp {
                packet: data.packet,
                source,
            }],
            true,
        )))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "prepared protocol IP source disappeared",
        )
        .into())
    }
}
