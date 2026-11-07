use super::*;
fn config(limit: usize) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&format!(
        r#"{{"runtime":{{"packet_route_max_routes":{limit}}},"route":{{"final":{{"type":"direct"}}}}}}"#,
    )).unwrap()
}
fn route(engine: &Engine) -> ApiResult<PacketRouteLease> {
    engine.register_packet_route(PacketRouteSnapshot {
        route_id: String::new(),
        core_instance_id: String::new(),
        config_revision: 0,
        inbound_tag: "tun".into(),
        outbound_tag: "direct".into(),
        endpoints: vec![],
        source: "10.0.0.2".into(),
        destination: "203.0.113.7".into(),
        ip_protocol: 17,
        translated: false,
        started_at_unix_ms: 0,
        state: PacketRouteState::Active,
    })
}
fn busy(error: crate::EngineError) {
    let crate::EngineError::Io(error) = error else {
        panic!("actual route owner must reject busy publication")
    };
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}
#[test]
fn configured_same_route_owner_counts_execution_and_response_return_without_evicting() {
    let engine = Engine::new(config(1)).unwrap();
    let first = route(&engine).unwrap();
    assert_eq!(route(&engine).err().unwrap().code, ApiErrorCode::Conflict);
    engine.reload_runtime_config(config(2)).unwrap();
    let second = route(&engine).unwrap();
    let control = second.control();
    control.responses_started();
    let revision = engine.config_revision();
    busy(engine.reload_runtime_config(config(1)).unwrap_err());
    assert_eq!(engine.config_revision(), revision);
    assert_eq!(engine.config().runtime.packet_route_max_routes, Some(2));
    assert!(!first.control().is_closed());
    assert!(!control.is_closed());
    drop(second);
    assert!(control.control.released.get());
    assert!(!control.control.responses.get());
    busy(engine.reload_runtime_config(config(1)).unwrap_err());
    assert_eq!(engine.packet_routes_snapshot(&Default::default()).total, 2);
    control.responses_stopped();
    engine.reload_runtime_config(config(1)).unwrap();
    assert_eq!(engine.packet_routes_snapshot(&Default::default()).total, 1);
    assert!(!first.control().is_closed());
    assert!(route(&engine).is_err());
    drop(first);
    let _recovered = route(&engine).unwrap();
}
#[test]
fn final_publication_rechecks_new_originals_and_rollback_restores_the_actual_limit() {
    let engine = Engine::new(config(2)).unwrap();
    let first = route(&engine).unwrap();
    let prepared = zero_runtime::configuration::PreparedConfiguration::prepare_owned(
        config(1),
        engine.configuration_source_owner(),
    )
    .unwrap();
    let candidate = engine.prepare_configuration_snapshot(prepared).unwrap();
    let second = route(&engine).unwrap();
    busy(engine.publish_prepared_snapshot(candidate).unwrap_err());
    assert_eq!(engine.config().runtime.packet_route_max_routes, Some(2));
    assert_eq!(engine.packet_routes.limit.load(Ordering::Relaxed), 2);
    let last_good = engine.runtime_snapshot();
    drop(second);
    engine.stage_runtime_config(config(1)).unwrap();
    assert!(route(&engine).is_err());
    engine.restore_staged_snapshot(last_good, false).unwrap();
    let _same_owner_growth = route(&engine).unwrap();
    assert!(!first.control().is_closed());
}
