use super::ProbeKey;
use super::{probe_timeout_ms_for_dns, OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS};
use crate::runtime::tcp_dispatch::TcpDispatchIntent;

#[test]
fn shared_probe_identity_separates_policy_and_diagnostic_intents() {
    let key = |intent| ProbeKey {
        config_identity: 7,
        target_tag: "node-a".to_owned(),
        url: "https://example.com/generate_204".to_owned(),
        intent,
    };

    assert_ne!(
        key(TcpDispatchIntent::PolicyProbe),
        key(TcpDispatchIntent::DiagnosticProbe),
        "a diagnostic probe must never join a policy probe that can be rejected by traffic quarantine"
    );
}

#[test]
fn probe_deadline_covers_the_complete_node_dns_fallback_chain() {
    let dns = serde_json::from_value::<zero_config::DnsConfig>(serde_json::json!({
        "servers": {
            "cloudflare-bootstrap": {
                "type": "doh",
                "host": "cloudflare-dns.com",
                "bootstrap": ["1.1.1.1"]
            },
            "google-bootstrap": {
                "type": "doh",
                "host": "dns.google",
                "bootstrap": ["8.8.8.8"]
            },
            "system": { "type": "system" }
        },
        "default_server": "cloudflare-bootstrap",
        "policy": {
            "timeout_ms": 5000,
            "node_server": "cloudflare-bootstrap",
            "node_fallback_servers": ["google-bootstrap", "system"]
        }
    }))
    .unwrap();

    assert_eq!(probe_timeout_ms_for_dns(Some(&dns)), 20_000);
}

#[test]
fn probe_deadline_uses_per_server_timeouts_and_deduplicates_fallbacks() {
    let dns = serde_json::from_value::<zero_config::DnsConfig>(serde_json::json!({
        "servers": {
            "system": { "type": "system" },
            "backup": { "type": "system" }
        },
        "default_server": "system",
        "policy": {
            "timeout_ms": 5000,
            "server_timeout_ms": { "system": 1000, "backup": 2000 },
            "node_server": "system",
            "node_fallback_servers": ["system", "backup"]
        }
    }))
    .unwrap();

    assert_eq!(probe_timeout_ms_for_dns(Some(&dns)), 8_000);
    assert_eq!(
        probe_timeout_ms_for_dns(None),
        OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS
    );
}
