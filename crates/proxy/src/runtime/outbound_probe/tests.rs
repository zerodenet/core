use super::ProbeKey;
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
