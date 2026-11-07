//! Concrete security-history cold assembly, outside the generic host.
//! This classifies configuration effects; it creates no executor or book.
use zero_config::RuntimeConfig;

pub(crate) fn ss_source_resources_changed(old: &RuntimeConfig, next: &RuntimeConfig) -> bool {
    let configured = old
        .inbounds
        .iter()
        .chain(&next.inbounds)
        .any(|input| input.protocol.protocol_name() == "shadowsocks")
        || old
            .outbounds
            .iter()
            .chain(&next.outbounds)
            .any(|input| input.protocol.protocol_name() == "shadowsocks");
    let responses_changed = old.runtime.effective_ss_udp_max_pending_responses()
        != next.runtime.effective_ss_udp_max_pending_responses()
        || old.runtime.effective_ss_udp_total_max_pending_responses()
            != next.runtime.effective_ss_udp_total_max_pending_responses();
    configured
        && (responses_changed
            || match (
                zero_runtime::configuration::SsSecurityBudgetLimits::from_runtime(&old.runtime),
                zero_runtime::configuration::SsSecurityBudgetLimits::from_runtime(&next.runtime),
            ) {
                (Ok(old), Ok(next)) => old != next,
                _ => true,
            })
}
