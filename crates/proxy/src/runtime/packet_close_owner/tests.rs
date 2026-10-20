use super::*;
use zero_api::{EventFilter, PacketRouteSnapshot, PacketRouteState};
fn engine() -> Engine {
    Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn route(
    engine: &Engine,
) -> (
    zero_engine::PacketRouteLease,
    zero_engine::PacketRouteControl,
    PacketRouteCloseCommand,
) {
    let before = engine
        .packet_routes_snapshot(&Default::default())
        .routes
        .into_iter()
        .map(|snapshot| snapshot.route_id)
        .collect::<std::collections::BTreeSet<_>>();
    let lease = engine
        .register_packet_route(PacketRouteSnapshot {
            route_id: String::new(),
            core_instance_id: String::new(),
            config_revision: 0,
            inbound_tag: "test".into(),
            outbound_tag: "test".into(),
            endpoints: vec![],
            source: "10.0.0.1".into(),
            destination: "10.0.0.2".into(),
            ip_protocol: 17,
            translated: false,
            started_at_unix_ms: 0,
            state: PacketRouteState::Active,
        })
        .unwrap();
    let control = lease.control();
    control.responses_started();
    let snapshot = engine
        .packet_routes_snapshot(&Default::default())
        .routes
        .into_iter()
        .find(|snapshot| !before.contains(&snapshot.route_id))
        .unwrap();
    (
        lease,
        control,
        PacketRouteCloseCommand {
            route_id: snapshot.route_id,
            expected_core_instance_id: engine.core_instance_id().into(),
            expected_config_revision: None,
        },
    )
}
fn closed(engine: &Engine) -> usize {
    engine
        .events_snapshot(&EventFilter::default())
        .iter()
        .filter(|event| event.event_type == zero_api::event_type::PACKET_ROUTE_CLOSED)
        .count()
}

#[tokio::test]
async fn cancelled_ack_keeps_the_actual_two_part_receipt_and_releases_apply_lock_after_timeout() {
    let engine = engine();
    let (lease, control, request) = route(&engine);
    let owner = Arc::new(PacketCloseOwner::new(engine.packet_route_capacity()));
    let serial = Arc::new(Mutex::new(()));
    let caller = {
        let (owner, engine, serial) = (owner.clone(), engine.clone(), serial.clone());
        tokio::spawn(async move {
            owner
                .close(engine, request, serial, Duration::from_millis(5))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), control.cancelled())
        .await
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let guard = tokio::time::timeout(Duration::from_secs(2), serial.lock())
        .await
        .unwrap();
    drop(guard);
    assert_eq!(closed(&engine), 0);
    assert!(owner
        .shutdown(Instant::now() + Duration::from_millis(5))
        .await
        .is_err());
    drop(lease);
    assert_eq!(
        closed(&engine),
        0,
        "lease release alone is not the response-pump acknowledgement"
    );
    assert!(owner
        .shutdown(Instant::now() + Duration::from_millis(5))
        .await
        .is_err());
    control.responses_stopped();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(closed(&engine), 1);
    assert_eq!(owner.owner.lock().await.active, 0);
}

#[tokio::test]
async fn full_receipt_scope_rejects_before_claiming_another_real_route() {
    let engine = engine();
    let (first, first_control, request) = route(&engine);
    let owner = Arc::new(PacketCloseOwner::new(NonZeroUsize::new(1).unwrap()));
    let serial = Arc::new(Mutex::new(()));
    assert!(owner
        .close(
            engine.clone(),
            request,
            serial.clone(),
            Duration::from_millis(5)
        )
        .await
        .is_err());
    let (second, second_control, second_request) = route(&engine);
    assert!(owner
        .close(
            engine.clone(),
            second_request.clone(),
            serial.clone(),
            Duration::from_millis(5)
        )
        .await
        .is_err());
    assert!(
        !second_control.is_closed(),
        "capacity rejection must precede the actual close claim"
    );
    assert_eq!(owner.owner.lock().await.active, 1);
    assert_eq!(
        owner.completed.load(Ordering::Acquire),
        0,
        "an unaccepted future is not a task completion"
    );
    drop(first);
    first_control.responses_stopped();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(owner
        .close(
            engine.clone(),
            second_request,
            serial,
            Duration::from_millis(5)
        )
        .await
        .is_err());
    assert!(
        !second_control.is_closed(),
        "stopped owner cannot reopen admission"
    );
    drop(second);
    second_control.responses_stopped();
    assert_eq!(closed(&engine), 1);
}

#[tokio::test]
async fn cancelled_failure_poll_keeps_both_actual_task_ids_and_sources() {
    use std::future::Future;
    let owner = Arc::new(PacketCloseOwner::new(NonZeroUsize::new(2).unwrap()));
    let mut state = owner.owner.lock().await;
    state.scope = Some(TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    ));
    let mut actual = Vec::new();
    for _ in 0..2 {
        let completion = owner.clone();
        let id = state
            .scope
            .as_ref()
            .unwrap()
            .registrar()
            .spawn(TaskClass::Critical, async move {
                let _completion = Completion(completion);
                panic!("actual packet receipt task failure")
            })
            .unwrap();
        state.active += 1;
        actual.push(id);
    }
    drop(state);
    tokio::time::timeout(Duration::from_secs(2), async {
        while owner.completed.load(Ordering::Acquire) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let state = owner.owner.lock().await;
    let mut observe = Box::pin(owner.observe_failure());
    std::future::poll_fn(|cx| {
        assert!(observe.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(observe);
    assert_eq!(owner.completed.load(Ordering::Acquire), 2);
    drop(state);
    let message = tokio::time::timeout(Duration::from_secs(2), owner.observe_failure())
        .await
        .unwrap();
    assert!(message.contains("failed"));
    let state = owner.owner.lock().await;
    assert_eq!(state.active, 0);
    assert_eq!(state.failures.len(), 2);
    for id in actual {
        assert!(
            state
                .failures
                .iter()
                .any(|failure| failure.id == id
                    && std::error::Error::source(&failure.error).is_some())
        );
    }
    drop(state);
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
}
