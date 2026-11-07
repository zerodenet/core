//! Complete cold Source maxima. No native acquisition or background owner.
use std::num::NonZeroUsize;
use zero_config::{InboundConfig, InboundProtocolConfig as I, RuntimeConfig, RuntimeOptionsConfig};
use zero_platform_tokio::contract::component::InboundSourceLimits;
pub(crate) fn limits(
    runtime: &RuntimeOptionsConfig,
    defaults: InboundSourceLimits,
) -> InboundSourceLimits {
    let n = |value| NonZeroUsize::new(value).expect("validated Source budget");
    InboundSourceLimits {
        accepted_sockets: n(runtime.effective_inbound_max_accepted_sockets()),
        datagram_associations: n(runtime.effective_inbound_max_datagram_associations()),
        datagram_peers: n(
            runtime.effective_inbound_max_datagram_peers(defaults.datagram_peers.get())
        ),
        packets_per_peer: n(
            runtime.effective_inbound_max_packets_per_peer(defaults.packets_per_peer.get())
        ),
        pending_sends: n(runtime.effective_inbound_max_pending_sends(defaults.pending_sends.get())),
        bind_attempts: n(runtime.effective_inbound_max_bind_attempts()),
        target_sockets: n(runtime.effective_inbound_max_target_sockets()),
        file_sessions: n(runtime.effective_inbound_max_file_sessions()),
        handoff_probe_attempts: n(runtime.effective_inbound_max_handoff_probe_attempts()),
    }
}
fn used(input: &InboundConfig, runtime: &RuntimeOptionsConfig) -> Vec<usize> {
    let actual = limits(runtime, InboundSourceLimits::default());
    let mut used = Vec::new();
    match &input.protocol {
        I::HttpConnect | I::Vmess { .. } | I::Trojan { .. } => {
            used.extend([actual.accepted_sockets.get(), actual.bind_attempts.get()])
        }
        I::Socks5 { .. } | I::Mixed { .. } => used.extend([
            actual.accepted_sockets.get(),
            actual.bind_attempts.get(),
            runtime.effective_inbound_max_pending_sends(1),
        ]),
        I::Shadowsocks { plugin, .. } => {
            used.extend([
                actual.accepted_sockets.get(),
                actual.bind_attempts.get(),
                runtime.effective_inbound_max_datagram_peers(4096),
                runtime.effective_inbound_max_packets_per_peer(32),
                actual.pending_sends.get(),
            ]);
            if plugin.is_some() {
                used.push(actual.handoff_probe_attempts.get());
            }
        }
        I::Mieru { transport, .. } => {
            used.push(actual.bind_attempts.get());
            if matches!(transport, zero_config::MieruTransport::Udp) {
                used.extend([
                    actual.datagram_peers.get(),
                    actual.packets_per_peer.get(),
                    actual.pending_sends.get(),
                ]);
            } else {
                used.push(actual.accepted_sockets.get());
            }
        }
        I::Vless {
            quic,
            hysteria,
            mkcp,
            fallback,
            reality,
            ..
        } => {
            used.push(actual.datagram_associations.get());
            if quic.is_none() && hysteria.is_none() {
                used.push(actual.bind_attempts.get());
                if mkcp.is_some() {
                    used.extend([
                        actual.datagram_peers.get(),
                        actual.packets_per_peer.get(),
                        actual.pending_sends.get(),
                    ]);
                } else {
                    used.push(actual.accepted_sockets.get());
                }
            }
            if fallback.is_some()
                || reality.is_some()
                || hysteria.as_ref().is_some_and(|h| {
                    matches!(
                        h.masquerade,
                        zero_config::HysteriaCarrierMasqueradeConfig::Proxy { .. }
                    )
                })
            {
                used.push(actual.target_sockets.get());
            }
            if hysteria.as_ref().is_some_and(|h| {
                matches!(
                    h.masquerade,
                    zero_config::HysteriaCarrierMasqueradeConfig::File { .. }
                )
            }) {
                used.push(actual.file_sessions.get());
            }
        }
        I::Hysteria2 { masquerade, .. } => {
            used.push(actual.datagram_associations.get());
            if masquerade.http.is_some() || masquerade.https.is_some() {
                used.extend([actual.accepted_sockets.get(), actual.bind_attempts.get()]);
            }
            match &masquerade.response {
                zero_config::Hysteria2MasqueradeResponseConfig::File { .. } => {
                    used.push(actual.file_sessions.get())
                }
                zero_config::Hysteria2MasqueradeResponseConfig::Proxy { .. } => {
                    used.push(actual.target_sockets.get())
                }
                _ => {}
            }
        }
        _ => {}
    }
    used
}
pub(crate) fn source_changed(
    input: &InboundConfig,
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    used(input, &old.runtime) != used(input, &next.runtime)
}
pub(crate) fn changed(old: &RuntimeConfig, next: &RuntimeConfig) -> bool {
    old.inbounds
        .iter()
        .chain(&next.inbounds)
        .any(|input| source_changed(input, old, next))
}
