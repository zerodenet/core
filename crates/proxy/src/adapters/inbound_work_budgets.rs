//! Concrete cold Source classification, outside generic runtime.
use zero_config::{InboundConfig, RuntimeConfig};
/// Actual current producer defaults. No Packet/TUN role is made an accepted
/// carrier, and sequential HTTP/SOCKS/SS framing gets no new concurrency.
fn inbound_work_defaults(input: &InboundConfig) -> Option<(usize, usize, Option<usize>)> {
    use zero_config::InboundProtocolConfig as I;
    Some(match &input.protocol {
        I::HttpConnect => (1, 0, None),
        I::Socks5 { .. } | I::Mixed { .. } => (2, 2, None),
        I::Shadowsocks { .. } => (1, 0, None),
        I::Mieru { .. } => (514, 257, Some(256)),
        I::Vmess { .. } | I::Trojan { .. } => (516, 257, Some(256)),
        I::Vless { mkcp, .. } => (520, 257, Some(if mkcp.is_some() { 128 } else { 256 })),
        I::Hysteria2 { .. } => (520, 257, Some(256)),
        _ => return None,
    })
}
pub(crate) fn inbound_work_source_changed(
    input: &InboundConfig,
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    let Some((tasks, resources, requests)) = inbound_work_defaults(input) else {
        return false;
    };
    let a = &old.runtime;
    let b = &next.runtime;
    a.effective_inbound_max_tasks_per_carrier(tasks)
        != b.effective_inbound_max_tasks_per_carrier(tasks)
        || a.effective_inbound_max_resources_per_carrier(resources)
            != b.effective_inbound_max_resources_per_carrier(resources)
        || requests.is_some_and(|default| {
            a.effective_inbound_max_in_flight_requests(default)
                != b.effective_inbound_max_in_flight_requests(default)
        })
}
pub(crate) fn inbound_work_budget_changed(old: &RuntimeConfig, next: &RuntimeConfig) -> bool {
    old.inbounds
        .iter()
        .chain(&next.inbounds)
        .any(|input| inbound_work_source_changed(input, old, next))
}
