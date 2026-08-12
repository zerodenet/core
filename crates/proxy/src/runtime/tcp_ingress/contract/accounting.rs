use crate::protocol_registry::TcpRuntimeServices;

pub(crate) fn record_tcp_upload(services: &TcpRuntimeServices, session_id: u64, bytes: u64) {
    services.record_session_inbound_rx(session_id, bytes);
    services.record_session_outbound_tx(session_id, bytes);
}

pub(crate) fn record_tcp_download(services: &TcpRuntimeServices, session_id: u64, bytes: u64) {
    services.record_session_outbound_rx(session_id, bytes);
    services.record_session_inbound_tx(session_id, bytes);
}
