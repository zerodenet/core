use crate::runtime::udp_dispatch::UdpDispatch;
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-stream-runtime"
))]
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamAssociationSend;
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-stream-runtime"
))]
use crate::runtime::udp_flow::snapshot::UdpFlowSnapshot;
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-stream-runtime"
))]
use zero_engine::EngineError;

impl UdpDispatch {
    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-stream-runtime"
    ))]
    pub(in crate::runtime::udp_dispatch) async fn forward_managed_relay_flow(
        &mut self,
        flow: &UdpFlowSnapshot,
        managed: ManagedUdpFlowRef,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        let services = self.runtime.runtime_services();
        let upstream = flow
            .outbound
            .upstream()
            .expect("relay flow should expose upstream endpoint");
        #[cfg(feature = "upstream-association-runtime")]
        if let Some(target) = self.flow_state.upstream_flow_target(managed) {
            return self
                .flow_state
                .start_upstream_flow(
                    &self.inbound_tag,
                    UpstreamAssociationSend {
                        services: Some(services.clone()),
                        session: &flow.session,
                        server: upstream.server,
                        port: upstream.port,
                        target,
                        payload,
                    },
                )
                .await
                .map_err(|failure| failure.error);
        }
        #[cfg(not(feature = "upstream-association-runtime"))]
        let _ = managed;
        #[cfg(feature = "managed-stream-runtime")]
        return self
            .flow_state
            .forward_existing_managed_flow(services, (flow, payload))
            .await
            .map_err(|failure| failure.error);

        #[cfg(not(any(
            feature = "managed-stream-runtime",
            feature = "managed-stream-runtime",
            feature = "managed-stream-runtime",
            feature = "managed-stream-runtime"
        )))]
        Err(EngineError::Io(std::io::Error::other(
            "registered upstream flow resume is not handled by the compiled adapter",
        )))
    }
}
