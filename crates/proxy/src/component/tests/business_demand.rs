use crate::{component::business_demand::AdmittedTcpDemand, Proxy};
use zero_core::{component::SessionId, Address, Network, ProtocolType, Session, SessionAuth};
use zero_platform_tokio::contract::context::RequestIdentity;

fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn session() -> Session {
    Session::new(
        0,
        Address::Domain("business.test".into()),
        443,
        Network::Tcp,
        ProtocolType::new("verified"),
    )
}
#[tokio::test]
async fn business_projection_and_public_session_handle_never_issue_admitted_demand() {
    let proxy = proxy();
    let services = proxy.tcp_runtime_services().for_inbound("in".into());
    let mut session = session();
    session.id = 99;
    let _public_handle = proxy.engine().track_session(session.id);
    let invocation = services
        .with_business_identity(&session)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    assert!(invocation.admitted.is_none());
    assert!(
        matches!(invocation.identity, RequestIdentity::Business(ref facts) if facts.session == SessionId(99))
    );
}
#[tokio::test]
async fn actual_issuer_and_original_quota_cancellation_survive_the_same_invocation() {
    let first = proxy();
    let foreign = proxy();
    let mut session = session();
    let mut auth = SessionAuth::new("verified");
    auth.principal_key = Some("actual-demand".into());
    auth.quota_remaining_bytes = Some(5);
    session.apply_auth(auth);
    let receipt = first
        .engine()
        .prepare_session_admitted(&mut session, "in")
        .unwrap();
    assert!(
        AdmittedTcpDemand::new(foreign.engine(), SessionId(session.id), receipt.clone()).is_err()
    );
    let demand = AdmittedTcpDemand::new(first.engine(), SessionId(session.id), receipt).unwrap();
    let cancel = demand.cancellation();
    let mut handle = first.engine().track_session(session.id);
    let (tx, rx) = tokio::sync::oneshot::channel();
    assert!(handle.register_cancellation(move || {
        let _ = tx.send(());
    }));
    let services = first
        .tcp_runtime_services()
        .for_inbound("in".into())
        .with_admitted_business_identity(&session, demand)
        .unwrap();
    let invocation = services
        .clone()
        .with_business_identity(&session)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    assert!(invocation.admitted.is_some());
    assert!(invocation.deadline.is_none());
    assert!(
        matches!(invocation.identity, RequestIdentity::Business(ref facts) if facts.session == SessionId(session.id))
    );
    let cloned = services.execution().invocation.unwrap();
    let (RequestIdentity::Business(original), RequestIdentity::Business(same)) =
        (&invocation.identity, &cloned.identity)
    else {
        panic!("actual business facts");
    };
    assert!(std::sync::Arc::ptr_eq(original, same));
    let propagated = invocation.cancellation();
    assert!(!propagated.is_cancelled());
    first.engine().record_session_inbound_rx(session.id, 5);
    rx.await.unwrap();
    assert!(cancel.is_cancelled());
    assert!(propagated.is_cancelled());
    handle
        .finish(zero_engine::SessionOutcome::Cancelled)
        .unwrap();
}
#[tokio::test]
async fn already_finalized_admission_projects_cancellation_without_authorizing_new_acquisition() {
    let proxy = proxy();
    let mut session = session();
    let receipt = proxy
        .engine()
        .prepare_session_admitted(&mut session, "in")
        .unwrap();
    proxy
        .engine()
        .track_session(session.id)
        .finish(zero_engine::SessionOutcome::DirectRelayed)
        .unwrap();
    assert!(proxy
        .engine()
        .check_admitted_session(&receipt, SessionId(session.id))
        .is_err());
    let demand = AdmittedTcpDemand::new(proxy.engine(), SessionId(session.id), receipt).unwrap();
    let invocation = proxy
        .tcp_runtime_services()
        .for_inbound("in".into())
        .with_admitted_business_identity(&session, demand)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    assert!(invocation.cancellation().is_cancelled());
    // This carries an already admitted identity/cancellation, not live permission.
    // NativeCarrierOpener's existing Service-only rejection remains unchanged.
}
