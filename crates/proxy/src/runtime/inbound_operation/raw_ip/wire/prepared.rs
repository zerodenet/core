//! The existing Source wire body borrows an actual Stack tuple, commits the
//! same protocol operation, then consumes synchronously. No callback/task/pool.
use super::*;
use crate::runtime::raw_ip::PreparedWireReceive;
pub(super) async fn finish(
    codec: &mut dyn RawIpInboundDevice,
    endpoint: Option<&RawIpInboundEndpoint>,
    received: PreparedWireReceive<RawIpInboundDispatch>,
    source: Option<std::net::SocketAddr>,
    proxied_peer: Option<usize>,
    read: &zero_tun::packet_memory::NativeReadGrant,
    traffic: &WireTraffic,
) -> Result<Option<RawIpInboundDispatch>, EngineError> {
    let mut data = match received {
        PreparedWireReceive::Complete(dispatch) => return Ok(Some(dispatch)),
        PreparedWireReceive::Data(data) => data,
    };
    let peer = data.peer;
    let expected = proxied_peer.is_none() || proxied_peer == peer;
    if data.allowed && expected {
        let device = peer.and_then(|peer| {
            endpoint.and_then(|endpoint| endpoint.peers.borrow().devices.get(peer).cloned())
        });
        if let Some(device) = device {
            let prepared = device
                .prepare_correlated_receive(&data.packet, read, traffic.allowed())
                .await
                .map_err(std::io::Error::other)?;
            if let Some(prepared) = prepared {
                // No Tunn/device mutex is held across the table-lock await.
                // Here the actual table guard stays held only across the short
                // final sameSession+replay commit and synchronous TCP consume.
                if let Err(error) = codec.commit_prepared_datagram(&data, source) {
                    tracing::debug!(
                        ?error,
                        "prepared wire Data lost actual Session/replay admission"
                    );
                    return Ok(None);
                }
                let prepared = prepared.release_return_fence();
                traffic
                    .peer(peer)
                    .rx(TrafficPlane::Inner, data.packet.len(), true);

                device.observe_authenticated_packet();
                assert!(
                    prepared.consume(),
                    "same actual tuple stayed locked through commit"
                );
                return Ok(Some(dispatch(peer, Vec::new(), 0)));
            }
        }
        // Only an unmatched/fragment/non-TCP packet enters the original
        // forwarded-packet prepay path, still before replay commit.
        if let Err(error) = data.admit_forward() {
            let failure = data.rejected(error);
            if matches!(
                error,
                zero_traits::PacketDeriveError::Busy | zero_traits::PacketDeriveError::Capacity
            ) {
                traffic.peer(peer).dropped_reason(
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
    if let Err(error) = codec.commit_prepared_datagram(&data, source) {
        tracing::debug!(
            ?error,
            "prepared wire Data lost actual Session/replay admission"
        );
        return Ok(None);
    }
    let actions = if data.allowed {
        vec![RawIpInboundAction::ReceiveIp(data.packet)]
    } else {
        Vec::new()
    };
    Ok(Some(dispatch(peer, actions, u64::from(!data.allowed))))
}
fn dispatch(
    peer: Option<usize>,
    actions: Vec<RawIpInboundAction>,
    rejected: u64,
) -> RawIpInboundDispatch {
    RawIpInboundDispatch {
        peer_index: peer,
        authenticated: true,
        actions,
        source_rejected_packets: rejected,
    }
}
