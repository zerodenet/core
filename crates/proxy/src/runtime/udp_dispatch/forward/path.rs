use std::time::Instant;

use zero_engine::EngineError;

use super::super::UdpDispatch;
use crate::runtime::path::UdpPathCategory;
use crate::runtime::udp_dispatch::model::UdpFlowCancellation;
use crate::runtime::udp_flow::snapshot::UdpFlowSnapshot;

impl UdpDispatch {
    /// Forward a packet to an existing flow.
    ///
    /// Dispatches by [`UdpPathCategory`] first, then by protocol-neutral flow
    /// accessors or `UdpFlowState`.
    pub(in crate::runtime::udp_dispatch) async fn forward_existing(
        &mut self,
        flow: &UdpFlowSnapshot,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let services = self.runtime.runtime_services();
        let started_at = Instant::now();
        if !flow.rate_limiters.throttle_upload(payload.len()).await {
            self.finish_cancelled_flow(UdpFlowCancellation::new(flow.session.id, false));
            return Err(EngineError::Io(std::io::Error::other(
                "UDP flow cancelled while upload was rate limited",
            )));
        }
        services.record_session_inbound_rx(flow.session.id, payload.len() as u64);

        match flow.outbound.path_category() {
            UdpPathCategory::Direct => {
                let Some(target_addr) = flow.outbound.direct_target_addr() else {
                    unreachable!("Direct category maps to Direct variant only");
                };
                match self.send_direct_packet(target_addr, payload).await {
                    Ok(sent) => {
                        services.record_session_outbound_tx(flow.session.id, sent as u64);
                    }
                    Err(error) => {
                        self.fail_flow(flow, started_at, "udp_direct_send", &error);
                        return Err(error);
                    }
                }
            }

            #[cfg(any(
                feature = "upstream-association-runtime",
                feature = "managed-stream-runtime"
            ))]
            UdpPathCategory::Relay => {
                let Some(managed) = flow.outbound.relay_managed_flow() else {
                    unreachable!("Relay category maps to a managed relay flow");
                };
                match self
                    .forward_managed_relay_flow(flow, managed, payload)
                    .await
                {
                    Ok(sent) => {
                        services.record_session_outbound_tx(flow.session.id, sent as u64);
                    }
                    Err(error) => {
                        self.fail_flow(flow, started_at, "udp_upstream_send", &error);
                        return Err(error);
                    }
                }
            }

            #[cfg(feature = "managed-datagram-runtime")]
            UdpPathCategory::Datagram => {
                let result = self
                    .flow_state
                    .forward_existing_managed_flow(services.clone(), (flow, payload))
                    .await;
                self.record_or_fail(flow, &services, started_at, result)?;
            }

            #[cfg(feature = "managed-stream-runtime")]
            UdpPathCategory::StreamPacket => {
                let result = self
                    .flow_state
                    .forward_existing_managed_flow(services.clone(), (flow, payload))
                    .await;
                self.record_or_fail(flow, &services, started_at, result)?;
            }

            #[cfg(feature = "udp-runtime")]
            UdpPathCategory::PacketPathDatagram => {
                let result = self
                    .flow_state
                    .forward_existing_packet_path_flow(flow, payload)
                    .await;
                self.record_or_fail(flow, &services, started_at, result)?;
            }
        }

        Ok(())
    }
}
