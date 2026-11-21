//! Identity projection tests; no synthetic caller or native-close evidence.
use super::*;
use crate::component::{business_demand::AdmittedBusinessDemand, direct_bridge::Invocation};
use zero_core::{component::SessionId, Address, Network, ProtocolType, Session};
use zero_platform_tokio::contract::context::RequestIdentity;

fn proxy() -> crate::Proxy {
    crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}

fn admitted(proxy: &crate::Proxy) -> (Session, Invocation) {
    let mut session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 1]),
        53,
        Network::Udp,
        ProtocolType::new("test"),
    );
    let receipt = proxy
        .engine()
        .prepare_session_admitted(&mut session, "in")
        .unwrap();
    let demand =
        AdmittedBusinessDemand::new(proxy.engine(), SessionId(session.id), receipt).unwrap();
    let call = proxy
        .tcp_runtime_services()
        .for_inbound("in".into())
        .with_admitted_business_identity(&session, demand)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    (session, call)
}

#[tokio::test]
async fn udp_continuation_preserves_original_facts_without_inventing_a_native_caller() {
    let proxy = proxy();
    let (session, original) = admitted(&proxy);
    let resumed = proxy
        .tcp_runtime_services()
        .resume_business_identity(&session, &original)
        .unwrap()
        .execution();
    let actual = resumed.invocation.as_ref().unwrap();
    assert!(matches!((&actual.identity, &original.identity),
        (RequestIdentity::Business(actual), RequestIdentity::Business(original))
            if Arc::ptr_eq(actual, original)));
    actual
        .admitted
        .as_ref()
        .unwrap()
        .check_live(proxy.engine(), SessionId(session.id))
        .unwrap();
    let failure = match crate::component::stream_bridge::actual_protocol_caller(&resumed) {
        Err(failure) => failure,
        Ok(_) => panic!("restoring admission fabricated a native task caller"),
    };
    assert_eq!(failure.reason, Reason::StalePlan);
}

#[tokio::test]
async fn udp_continuation_rejects_foreign_sessions_engines_and_admitted_invocations() {
    let proxy = proxy();
    let other = self::proxy();
    let (session, original) = admitted(&proxy);
    let (another, another_call) = admitted(&proxy);
    assert!(proxy
        .tcp_runtime_services()
        .resume_business_identity(&another, &original)
        .is_err());
    assert!(other
        .tcp_runtime_services()
        .resume_business_identity(&session, &original)
        .is_err());
    let mut occupied = proxy.tcp_runtime_services();
    occupied.execution.invocation = Some(another_call);
    assert!(occupied
        .resume_business_identity(&session, &original)
        .is_err());
    let mut missing_receipt = original.clone();
    missing_receipt.admitted = None;
    assert!(proxy
        .tcp_runtime_services()
        .resume_business_identity(&session, &missing_receipt)
        .is_err());
}
