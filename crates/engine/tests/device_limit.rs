use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};
use zero_engine::{Engine, EngineError, SessionHandle, SessionOutcome};

fn engine() -> Engine {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    Engine::new(config).expect("build engine")
}

fn admit(
    engine: &Engine,
    principal_key: &str,
    source_ip: Option<Address>,
    device_limit: u32,
) -> Result<SessionHandle, EngineError> {
    let mut session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    session.source_ip = source_ip;
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some(principal_key.to_owned());
    auth.device_limit = Some(device_limit);
    session.apply_auth(auth);
    engine.prepare_session(&mut session, "vless-in")?;
    Ok(engine.track_session(session.id))
}

#[test]
fn device_limit_counts_unique_source_ips_across_concurrent_flows() {
    let engine = engine();
    let first_ip = Address::Ipv4([192, 0, 2, 1]);
    let second_ip = Address::Ipv4([192, 0, 2, 2]);

    let mut first = admit(&engine, "account:1", Some(first_ip.clone()), 1).unwrap();
    let mut duplicate = admit(&engine, "account:1", Some(first_ip), 1).unwrap();
    assert!(matches!(
        admit(&engine, "account:1", Some(second_ip.clone()), 1),
        Err(EngineError::AdmissionDenied { .. })
    ));

    first.finish(SessionOutcome::DirectRelayed).unwrap();
    assert!(matches!(
        admit(&engine, "account:1", Some(second_ip.clone()), 1),
        Err(EngineError::AdmissionDenied { .. })
    ));

    duplicate.finish(SessionOutcome::DirectRelayed).unwrap();
    let mut second = admit(&engine, "account:1", Some(second_ip), 1).unwrap();
    second.finish(SessionOutcome::DirectRelayed).unwrap();
}

#[test]
fn device_limited_session_requires_an_observable_source_ip() {
    let engine = engine();

    assert!(matches!(
        admit(&engine, "account:1", None, 1),
        Err(EngineError::AdmissionDenied { .. })
    ));
    assert!(engine.active_sessions().is_empty());
}

#[test]
fn zero_device_limit_is_treated_as_unlimited() {
    let engine = engine();
    let mut first = admit(&engine, "account:1", Some(Address::Ipv4([192, 0, 2, 1])), 0).unwrap();
    let mut second = admit(&engine, "account:1", Some(Address::Ipv4([192, 0, 2, 2])), 0).unwrap();

    first.finish(SessionOutcome::DirectRelayed).unwrap();
    second.finish(SessionOutcome::DirectRelayed).unwrap();
}

#[test]
fn carrier_and_child_flows_share_the_same_device_reference() {
    let engine = engine();
    let first_ip = Address::Ipv4([192, 0, 2, 1]);
    let second_ip = Address::Ipv4([192, 0, 2, 2]);
    let mut auth = SessionAuth::new("hysteria2");
    auth.principal_key = Some("account:1".to_owned());
    auth.device_limit = Some(1);

    let carrier = engine
        .acquire_principal_device(Some(&auth), Some(&first_ip))
        .unwrap()
        .expect("carrier device registration");
    let mut child = admit(&engine, "account:1", Some(first_ip), 1).unwrap();
    assert!(matches!(
        admit(&engine, "account:1", Some(second_ip.clone()), 1),
        Err(EngineError::AdmissionDenied { .. })
    ));

    drop(carrier);
    assert!(matches!(
        admit(&engine, "account:1", Some(second_ip.clone()), 1),
        Err(EngineError::AdmissionDenied { .. })
    ));
    child.finish(SessionOutcome::DirectRelayed).unwrap();

    let mut admitted = admit(&engine, "account:1", Some(second_ip), 1).unwrap();
    admitted.finish(SessionOutcome::DirectRelayed).unwrap();
}
