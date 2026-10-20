//! Actual host, admitted UDP session, resolved Direct leaf and registered peer.
pub(crate) mod fixture;
use super::*;
use fixture::*;

#[tokio::test]
async fn actual_direct_source_requires_published_host_session_and_same_registered_peer() {
    let live = Live::new().await;
    let source = live.source().await;
    let generation = NetworkGeneration(
        live.runtime
            .services()
            .upstream()
            .egress_control()
            .generation(),
    );
    source.check_current(generation).unwrap();
    let mut unadmitted = live.session.clone();
    unadmitted.id = 0;
    assert_eq!(
        live.operation
            .socket_source(
                live.proxy.current_service_source(),
                &live.runtime,
                &unadmitted,
                &live.context,
            )
            .await
            .unwrap_err()
            .reason,
        Reason::Revoked
    );
    let foreign = crate::runtime::Proxy::new(config("auto")).unwrap();
    assert_eq!(
        live.operation
            .socket_source(
                foreign.current_service_source(),
                &live.runtime,
                &live.session,
                &live.context,
            )
            .await
            .unwrap_err()
            .reason,
        Reason::StalePlan
    );
    let no_peer = UdpIngressRuntime::new(
        live.proxy
            .tcp_runtime_services()
            .for_inbound("source".into()),
    );
    assert_eq!(
        live.operation
            .socket_source(
                live.proxy.current_service_source(),
                &no_peer,
                &live.session,
                &live.context,
            )
            .await
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    let egress = live.runtime.services().upstream().egress_control();
    egress.invalidate_network();
    assert_eq!(
        source.check_current(generation).unwrap_err().reason,
        Reason::StalePlan
    );
    live.close().await;
}

#[tokio::test]
async fn direct_source_keeps_no_host_config_and_policy_aba_never_revives_old_epoch() {
    let live = Live::new().await;
    let source = live.source().await;
    let generation = NetworkGeneration(
        live.runtime
            .services()
            .upstream()
            .egress_control()
            .generation(),
    );
    let host = live.proxy.current_service_source();
    let old_config = Arc::downgrade(&live.proxy.engine().config());
    // An unrelated snapshot change must not revoke a healthy Direct policy.
    let mut unrelated = config("auto");
    unrelated.inbounds.clear();
    live.proxy
        .engine()
        .reload_runtime_config(unrelated)
        .unwrap();
    source.check_current(generation).unwrap();
    live.proxy
        .engine()
        .reload_runtime_config(config("ipv4_only"))
        .unwrap();
    assert_eq!(
        source.check_current(generation).unwrap_err().reason,
        Reason::StalePlan
    );
    live.proxy
        .engine()
        .reload_runtime_config(config("auto"))
        .unwrap();
    assert_eq!(
        source.check_current(generation).unwrap_err().reason,
        Reason::StalePlan
    );
    live.close().await;
    assert!(
        host.upgrade().is_none(),
        "policy source only retains the real host Weak handle"
    );
    assert!(
        old_config.upgrade().is_none(),
        "policy source cannot pin old Config/Engine owners"
    );
}
