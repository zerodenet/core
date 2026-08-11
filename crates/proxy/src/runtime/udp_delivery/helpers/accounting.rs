use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;

fn record_udp_inbound_response_rx(
    services: &UdpRuntimeServices,
    session_id: Option<u64>,
    payload_len: usize,
) {
    if let Some(session_id) = session_id {
        services.record_session_outbound_rx(session_id, payload_len as u64);
    }
}

fn record_udp_inbound_response_tx(
    services: &UdpRuntimeServices,
    session_id: Option<u64>,
    written_len: usize,
) {
    if let Some(session_id) = session_id {
        services.record_session_inbound_tx(session_id, written_len as u64);
    }
}

pub(crate) struct UdpInboundResponseAccounting {
    services: UdpRuntimeServices,
    session_id: Option<u64>,
    rate_limiters: UdpFlowRateLimiters,
}

impl UdpInboundResponseAccounting {
    pub(crate) fn record_received(
        services: &UdpRuntimeServices,
        session_id: Option<u64>,
        payload_len: usize,
        rate_limiters: UdpFlowRateLimiters,
    ) -> Self {
        record_udp_inbound_response_rx(services, session_id, payload_len);
        Self {
            services: services.clone(),
            session_id,
            rate_limiters,
        }
    }

    pub(crate) async fn throttle_download(&self, payload_len: usize) -> bool {
        self.rate_limiters.throttle_download(payload_len).await
    }

    pub(crate) fn record_sent(&self, written_len: usize) {
        record_udp_inbound_response_tx(&self.services, self.session_id, written_len);
    }

    pub(crate) fn session_id(&self) -> Option<u64> {
        self.session_id
    }
}
