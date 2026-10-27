#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_dispatch::managed::model::UpstreamTrackedStart;
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_dispatch::FlowStartResult;
use crate::runtime::udp_dispatch::{FlowFailure, UdpDispatch};
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::outbound::UdpFlowOutbound;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamAssociationSend;

impl UdpDispatch {
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) async fn start_tracked_upstream(
        &mut self,
        request: UpstreamTrackedStart<'_>,
    ) -> Result<FlowStartResult, FlowFailure> {
        let services = request.services.as_ref().ok_or_else(|| FlowFailure {
            stage: "udp_upstream_proxy",
            error: zero_engine::EngineError::Io(std::io::Error::other(
                "expected proxy context for registered upstream UDP flow",
            )),
            upstream: Some((request.server.into(), request.port)),
        })?;
        let execution = services
            .datagram_execution(&self.inbound_tag, request.session)
            .map_err(|error| FlowFailure {
                stage: "udp_upstream_send",
                error,
                upstream: Some((request.server.into(), request.port)),
            })?;
        let target = crate::runtime::udp_flow::registered::DatagramAssociationTarget::resolve(
            &execution,
            request.tag,
            request.server,
            request.port,
        )
        .map_err(|error| FlowFailure {
            stage: "udp_upstream_send",
            error,
            upstream: Some((request.server.into(), request.port)),
        })?;
        let sent = self
            .flow_state
            .start_upstream_flow(
                &self.inbound_tag,
                UpstreamAssociationSend {
                    services: request.services,
                    session: request.session,
                    server: request.server,
                    port: request.port,
                    target: target.clone(),
                    payload: request.payload,
                },
            )
            .await?;
        let managed = self.flow_state.register_upstream_flow(target);
        Ok(FlowStartResult::Flow {
            outbound: Box::new(UdpFlowOutbound::Relay {
                tag: request.tag.to_owned(),
                server: request.server.to_owned(),
                port: request.port,
                managed,
            }),
            tx_bytes: sent as u64,
        })
    }
}
