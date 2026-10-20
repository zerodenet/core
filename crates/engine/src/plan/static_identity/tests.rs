use super::*;
use crate::Engine;
use serde_json::json;

fn config() -> RuntimeConfig {
    RuntimeConfig::parse(&json!({
        "outbounds":[
            {"tag":"selected","protocol":{"type":"socks5","server":"127.0.0.1","port":1080,"username":"user","password":"one"}},
            {"tag":"other","protocol":{"type":"direct"}}
        ], "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
fn capture(engine: &Engine) -> OutboundStaticBinding {
    engine
        .runtime_snapshot()
        .plan()
        .outbound_static_binding("selected")
        .unwrap()
}
#[test]
fn ordinary_rules_and_unrelated_outbounds_retain_the_selected_dependency() {
    let engine = Engine::new(config()).unwrap();
    let binding = capture(&engine);
    let other = engine
        .runtime_snapshot()
        .plan()
        .outbound_static_binding("other")
        .unwrap();
    let mut updated = config();
    updated.outbounds[1].dial.address_family = zero_config::OutboundAddressFamily::OnlyIpv4;
    // This changes rule generation without adding a dependency to this outbound.
    updated.route = serde_json::from_value(json!({"final":{"type":"reject"}})).unwrap();
    engine.reload_runtime_config(updated).unwrap();
    assert!(binding.matches(engine.runtime_snapshot().plan()));
    assert!(
        !other.matches(engine.runtime_snapshot().plan()),
        "the changed Direct dial is part of its own static options"
    );
    assert!(engine
        .runtime_snapshot()
        .plan()
        .outbound_static_binding("missing")
        .is_none());
}
#[test]
fn complete_option_changes_and_aba_never_revive_the_old_token() {
    let original = config();
    let engine = Engine::new(original.clone()).unwrap();
    let binding = capture(&engine);
    let mut changed = original.clone();
    if let zero_config::OutboundProtocolConfig::Socks5 { password, .. } =
        &mut changed.outbounds[0].protocol
    {
        *password = Some("two".into());
    } else {
        panic!("actual configured SOCKS outbound");
    }
    engine.reload_runtime_config(changed).unwrap();
    assert!(!binding.matches(engine.runtime_snapshot().plan()));
    engine.reload_runtime_config(original).unwrap();
    assert!(
        !binding.matches(engine.runtime_snapshot().plan()),
        "A -> B -> A has a new identity"
    );
    assert!(capture(&engine).matches(engine.runtime_snapshot().plan()));
}
#[test]
fn removing_and_readding_an_outbound_does_not_recycle_its_identity() {
    let engine = Engine::new(config()).unwrap();
    let binding = capture(&engine);
    let mut without = config();
    without.outbounds.remove(0);
    engine.reload_runtime_config(without).unwrap();
    assert!(!binding.matches(engine.runtime_snapshot().plan()));
    engine.reload_runtime_config(config()).unwrap();
    assert!(!binding.matches(engine.runtime_snapshot().plan()));
}
#[test]
fn udp_and_constructor_directory_changes_invalidate_static_options() {
    let original = config();
    let engine = Engine::new(original.clone()).unwrap();
    let binding = capture(&engine);
    let mut changed = original.clone();
    changed.outbounds[0].udp.enabled = false;
    engine.reload_runtime_config(changed).unwrap();
    assert!(!binding.matches(engine.runtime_snapshot().plan()));
    let binding = capture(&engine);
    let mut moved = engine.config().as_ref().clone();
    moved.source_dir = Some(std::path::PathBuf::from("/actual/other/source"));
    engine.reload_runtime_config(moved).unwrap();
    assert!(!binding.matches(engine.runtime_snapshot().plan()));
}
#[test]
fn weak_bindings_do_not_retain_engine_config_plan_or_unbounded_history() {
    let engine = Engine::new(config()).unwrap();
    let snapshot = engine.runtime_snapshot();
    let old_plan = Arc::downgrade(snapshot.plan());
    let old_config = Arc::downgrade(snapshot.config());
    let binding = capture(&engine);
    drop(snapshot);
    engine.reload_runtime_config(config()).unwrap();
    assert!(old_plan.upgrade().is_none());
    assert!(old_config.upgrade().is_none());
    assert!(binding.matches(engine.runtime_snapshot().plan()));
    drop(engine);
    assert!(binding.token.upgrade().is_none());
}
