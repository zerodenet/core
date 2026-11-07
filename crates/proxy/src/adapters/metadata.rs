//! Concrete cold assembly classification, outside the generic runtime.
use zero_config::{InboundConfig, RuntimeConfig};

/// A Source's own metadata declaration must physically retire before its
/// replacement takes these limits. Shared outbound endpoint changes do not
/// force unrelated sources to rebind; their canonical recipe retires itself.
pub(crate) fn metadata_source_budget_changed(
    input: &InboundConfig,
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    let a = &old.runtime;
    let b = &next.runtime;
    let local_changed = a.effective_metadata_max_probe_sockets(2)
        != b.effective_metadata_max_probe_sockets(2)
        || a.effective_metadata_max_probe_attempts(8) != b.effective_metadata_max_probe_attempts(8)
        || a.effective_metadata_max_send_calls(2) != b.effective_metadata_max_send_calls(2);
    let masked_mkcp = matches!(&input.protocol,
        zero_config::InboundProtocolConfig::Vless { mkcp: Some(_), final_mask: Some(mask), .. }
        if mask.udp.iter().any(|mask| matches!(mask, zero_config::UdpMaskConfig::Xdns { .. } | zero_config::UdpMaskConfig::Xicmp { .. })));
    let shared_quic = matches!(
        &input.protocol,
        zero_config::InboundProtocolConfig::Hysteria2 { .. }
            | zero_config::InboundProtocolConfig::Vless { quic: Some(_), .. }
    );
    (local_changed && (shared_quic || masked_mkcp))
        || (shared_quic
            && (a.effective_quic_source_max_connections()
                != b.effective_quic_source_max_connections()
                || a.effective_quic_shared_endpoint_max_users()
                    != b.effective_quic_shared_endpoint_max_users()))
}
