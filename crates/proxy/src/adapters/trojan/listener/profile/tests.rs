use crate::{
    adapters::trojan::{inbound_user_refs, TrojanAdapter},
    inventory::ProtocolInventory,
    protocol_registry::InboundListenerCapability,
    runtime::inbound_owners::InboundOwnerRegistry,
};
use zero_config::{InboundProtocolConfig, RuntimeConfig};

#[tokio::test]
async fn cold_prepare_drop_and_transport_failure_leave_live_users_until_install() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&cert, certificate.cert.pem()).unwrap();
    std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[{"tag":"trojan-in","listen":{"address":"127.0.0.1","port":18443},
                "protocol":{"type":"trojan","users":[{"password":"new"},{"password":"second"}],
                    "tls":{"cert_path":cert,"key_path":key}}}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let adapter = TrojanAdapter::default();
    let live = adapter
        .inbound_profiles
        .replace("trojan-in", &inbound_user_refs("old", &[]));
    let candidate = config.inbounds[0].clone();
    let discarded = adapter
        .prepare_inbound_listener(candidate.clone(), None)
        .unwrap();
    assert_eq!(
        live.user_count(),
        1,
        "cold preparation must not publish users"
    );
    drop(discarded);
    assert_eq!(live.user_count(), 1);
    let mut invalid = candidate.clone();
    let InboundProtocolConfig::Trojan { tls, .. } = &mut invalid.protocol else {
        panic!("Trojan");
    };
    *tls = None;
    assert!(adapter.prepare_inbound_listener(invalid, None).is_err());
    assert_eq!(
        live.user_count(),
        1,
        "transport validation cannot publish users"
    );

    let mut operation = adapter.prepare_inbound_listener(candidate, None).unwrap();
    assert_eq!(live.user_count(), 1);
    let owners = InboundOwnerRegistry::default();
    let protocols = ProtocolInventory::default();
    let (stop, _) = tokio::sync::watch::channel(false);
    let installed = owners
        .install_listener("trojan-in", &protocols, operation.internal_tasks(), stop)
        .unwrap();
    operation.install_resources(&installed).unwrap();
    assert_eq!(
        live.user_count(),
        2,
        "install publishes to the original profile clone"
    );
}
