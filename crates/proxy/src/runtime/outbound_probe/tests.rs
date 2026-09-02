use super::{
    probe_deadline_for_dns, OutboundProbeDeadline, OutboundProbeError, ProbeKey, SharedProbeEntry,
    SharedProbeWaiter, OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS,
};
use crate::runtime::tcp_dispatch::TcpDispatchIntent;
use futures_util::FutureExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
fn dropping_the_last_shared_probe_waiter_removes_the_pending_entry() {
    let key = ProbeKey {
        config_identity: 7,
        target_tag: "node-a".to_owned(),
        url: "https://example.com/generate_204".to_owned(),
        intent: TcpDispatchIntent::DiagnosticProbe,
    };
    let future = async { Ok::<u64, OutboundProbeError>(1) }.boxed().shared();
    let probes = Arc::new(Mutex::new(HashMap::from([(
        key.clone(),
        SharedProbeEntry {
            future: future.clone(),
            waiters: 1,
        },
    )])));

    drop(SharedProbeWaiter {
        probes: probes.clone(),
        key,
        future,
    });

    assert!(probes.lock().unwrap().is_empty());
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

    assert_eq!(
        probe_deadline_for_dns(Some(&dns)),
        OutboundProbeDeadline {
            dns_budget_ms: 15_000,
            transport_budget_ms: 5_000,
            timeout_ms: 20_000,
            capped: false,
        }
    );
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

    assert_eq!(
        probe_deadline_for_dns(Some(&dns)),
        OutboundProbeDeadline {
            dns_budget_ms: 3_000,
            transport_budget_ms: 5_000,
            timeout_ms: 8_000,
            capped: false,
        }
    );
    assert_eq!(
        probe_deadline_for_dns(None),
        OutboundProbeDeadline {
            dns_budget_ms: 0,
            transport_budget_ms: OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS,
            timeout_ms: OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS,
            capped: false,
        }
    );
}

#[test]
fn probe_deadline_reports_when_the_dns_chain_exceeds_the_hard_cap() {
    let dns = serde_json::from_value::<zero_config::DnsConfig>(serde_json::json!({
        "servers": {
            "system": { "type": "system" }
        },
        "default_server": "system",
        "policy": {
            "timeout_ms": 120000,
            "node_server": "system"
        }
    }))
    .unwrap();

    assert_eq!(
        probe_deadline_for_dns(Some(&dns)),
        OutboundProbeDeadline {
            dns_budget_ms: 120_000,
            transport_budget_ms: 5_000,
            timeout_ms: 60_000,
            capped: true,
        }
    );
}
