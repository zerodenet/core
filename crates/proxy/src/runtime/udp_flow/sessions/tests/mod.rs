use std::net::{Ipv4Addr, SocketAddr};

use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};
use zero_engine::{Engine, SessionOutcome};

use super::UdpSessionFlows;
use crate::runtime::udp_flow::outbound::UdpFlowOutbound;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;

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

#[test]
fn cancelled_udp_flow_finishes_with_reason_and_is_removed_from_lookup() {
    let engine = engine();
    let target = Address::Domain("example.com".to_owned());
    let mut session = Session::new(
        0,
        target.clone(),
        443,
        Network::Udp,
        ProtocolType::new("vless"),
    );
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some("account:1".to_owned());
    session.apply_auth(auth);
    engine
        .prepare_session(&mut session, "vless-in")
        .expect("session should be admitted");
    let session_id = session.id;
    let handle = engine.track_session(session_id);

    let mut flows = UdpSessionFlows::default();
    flows.insert(
        session,
        handle,
        UdpFlowOutbound::Direct {
            tag: "direct".to_owned(),
            target_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 443)),
        },
        None,
        Vec::new(),
        UdpFlowRateLimiters::default(),
    );
    assert!(flows.snapshot(&target, 443, None).is_some());

    assert_eq!(
        engine.close_principal_flows("account:1", "principal_disabled"),
        vec![session_id]
    );
    let completed = flows
        .finish_cancelled(session_id)
        .expect("finish cancelled flow");

    assert_eq!(completed.record.outcome, SessionOutcome::Cancelled);
    assert_eq!(
        completed.record.close_reason.as_deref(),
        Some("principal_disabled")
    );
    assert!(flows.snapshot(&target, 443, None).is_none());
    assert!(engine.active_sessions().is_empty());
}
