use super::*;
use crate::{
    component::business_demand::AdmittedTcpDemand,
    runtime::{inbound_owners::InboundOwnerRegistry, Proxy},
};
use zero_core::{component::SessionId, Address, Network, ProtocolType, Session};

#[tokio::test]
async fn same_actual_caller_reaches_the_admitted_invocation_but_real_join_never_closes_native_io() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("actual-caller", &proxy.protocols, 1, stop)
        .unwrap();
    let group = installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .unwrap();
    let host = installed.tasks();
    let owner = installed.owner();
    let native = NativeObligations::new(2, 1, 1);
    installed.register(native.resource()).unwrap();
    let mut permit = native.reserve(ChildKind::Tcp).unwrap();
    let registration = permit.registration();
    assert!(registration.check_open().is_err());
    let (stop_parent, parent_stopped) = oneshot::channel();
    owner
        .spawn_driver(async move {
            parent_stopped.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    let (invoked, invocation) = oneshot::channel();
    let (done, finished) = oneshot::channel();
    let engine = proxy.engine().clone();
    let services = proxy.tcp_runtime_services().for_inbound("in".into());
    let same = registration.clone();
    group
        .spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            permit.bind_caller(binding).map_err(std::io::Error::other)?;
            permit.enter().map_err(std::io::Error::other)?;
            let mut session = Session::new(
                0,
                Address::Ipv4([127, 0, 0, 1]),
                443,
                Network::Tcp,
                ProtocolType::new("actual-caller"),
            );
            let receipt = engine.prepare_session_admitted(&mut session, "in")?;
            let demand = AdmittedTcpDemand::new(&engine, SessionId(session.id), receipt)?;
            assert_ne!(session.id, 0);
            let execution = services
                .with_admitted_business_identity(&session, demand)?
                .with_caller_native(Some(same))?
                .execution();
            let original = execution.invocation.unwrap();
            assert!(original.admitted.is_some());
            invoked
                .send((session, original))
                .unwrap_or_else(|_| panic!("live invocation receiver"));
            finished.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    let (session, invocation) = invocation.await.unwrap();
    let observed = invocation.caller_native.as_ref().unwrap();
    assert!(observed.same_slot(&registration));
    assert!(observed.check_open().is_ok());
    assert!(registration.caller_joined().is_none());
    let other = native.reserve(ChildKind::Tcp).unwrap();
    assert!(!other.registration().same_slot(&registration));
    assert!(proxy
        .tcp_runtime_services()
        .for_inbound("in".into())
        .with_caller_native(Some(registration.clone()))
        .is_err());
    done.send(()).unwrap();
    stop_parent.send(()).unwrap();
    assert!(registry
        .confirm_shutdown("actual-caller", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    let joined = registration.caller_joined().unwrap();
    assert_eq!(
        joined.class(),
        zero_platform_tokio::contract::scope::TaskClass::Critical
    );
    assert!(joined.failure().is_none());
    assert!(Arc::ptr_eq(&joined, &registration.caller_joined().unwrap()));
    assert!(registration.check_open().is_err());
    assert!(invocation.cancellation().is_cancelled());
    assert!(owner.terminal_for_test().is_none());
    assert_eq!(native.0.lock().unwrap().records.len(), 2);
    proxy
        .engine()
        .track_session(session.id)
        .finish(zero_engine::SessionOutcome::Cancelled)
        .unwrap();
}
