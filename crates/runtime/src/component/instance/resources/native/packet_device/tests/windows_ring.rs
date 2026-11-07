//! Same real Host original/registrar, including no-privilege pre-acquisition cases.
use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::{io::AsyncReadExt, sync::oneshot};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::scope::{
    HostedScope, TaskClass, TaskRegistrationBudget, TaskScope,
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn scope(max: usize) -> (HostedScope, Arc<crate::HostOriginalBudgets>) {
    let mut roots = crate::HostOriginalBudgets::default();
    roots.component_tasks = TaskRegistrationBudget::new(NonZeroUsize::new(max).unwrap());
    let roots = Arc::new(roots);
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
    scope
        .bind_task_budget(roots.component_tasks.clone())
        .unwrap();
    scope.enable_registration().unwrap();
    (scope, roots)
}
fn owner(
    scope: &HostedScope,
    roots: Arc<crate::HostOriginalBudgets>,
    name: String,
) -> PacketDeviceOwner {
    PacketDeviceOwner::new(
        0,
        ComponentInstanceId {
            component: ComponentId(197),
            generation: ResourceGeneration(1),
        },
        scope.registrar().scope_identity(),
        "sdk-ring-test".into(),
        PacketDeviceBinding::Create {
            name: Some(name),
            addresses: vec![(
                "192.0.2.1".parse().unwrap(),
                "255.255.255.0".parse().unwrap(),
            )],
            mtu: 1500,
        },
        CancellationToken::new(),
        roots,
        Some(scope.registrar().downgrade()),
    )
    .unwrap()
}
fn driver(
    scope: &HostedScope,
) -> (
    oneshot::Sender<GrantedPacketDevice>,
    oneshot::Receiver<()>,
    TaskJoinWitness,
) {
    let (send, mut input) = oneshot::channel::<GrantedPacketDevice>();
    let (started, ready) = oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let mut started = Some(started);
            let grant = std::future::poll_fn(|cx| {
                let poll = std::pin::Pin::new(&mut input).poll(cx);
                if poll.is_pending() {
                    if let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                }
                poll
            })
            .await;
            if let Ok(mut grant) = grant {
                let mut bytes = [0; 65536];
                grant.io.read(&mut bytes).await.map_err(|source| {
                    error(ComponentId(197), Reason::Io, "actual original native read")
                        .with_source(source)
                })?;
            }
            Ok(())
        })
        .unwrap();
    (send, ready, witness)
}
#[tokio::test]
async fn full_global_launch_rejects_sdk_before_its_event_and_cancellation_keeps_original_request() {
    let (mut scope, roots) = scope(1);
    let owner = owner(&scope, roots.clone(), "ZeroRingNoWorker".into());
    let (send, ready, main) = driver(&scope);
    ready.await.unwrap();
    let failure = owner
        .open(main.clone())
        .await
        .err()
        .expect("global Critical launch is full");
    assert_eq!(failure.reason, Reason::Capacity);
    assert!(
        owner.original.physical_confirmed(None),
        "no event/adapter/session was acquired"
    );
    assert!(owner.memory.test_sdk_reader().is_none());
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 1);
    assert!(owner.memory.test_session_metadata_retained());
    let mut closing = Box::pin(owner.shutdown(end()));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 1);
    assert!(!owner.evidence().physical_confirmed());
    drop(send);
    let receipt = scope.next_receipt().await.unwrap();
    assert!(main.matches_joined(receipt.joined()));
    assert!(!receipt.joined().registration_released());
    assert!(owner.shutdown(end()).await.is_complete());
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 0);
    assert!(!owner.memory.test_session_metadata_retained());
    assert_eq!(
        roots.component_tasks.usage().charged,
        1,
        "main ACK is independent of NoTask SDK settlement"
    );
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert!(scope.next_receipt().await.is_none());
}
#[tokio::test]
async fn actual_sdk_worker_and_event_hold_request_until_the_same_failed_registration_ack() {
    let (mut scope, roots) = scope(2);
    // Original cold binding accepts the name, but the actual request validator
    // rejects >128 UTF-16 units on the real SDK worker BEFORE SDK/DLL access.
    let owner = owner(&scope, roots.clone(), "x".repeat(129));
    let (send, ready, main) = driver(&scope);
    ready.await.unwrap();
    assert!(owner.open(main.clone()).await.is_err());
    let sdk = owner
        .memory
        .test_sdk_reader()
        .expect("the actual blocking SDK worker");
    assert!(sdk.native_task_id().is_some());
    assert!(owner.original.ring_request_finished());
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 1);
    drop(send);
    let mut main_exit = None;
    let mut sdk_exit = None;
    for _ in 0..2 {
        let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
            .await
            .unwrap()
            .unwrap();
        if main.matches_joined(receipt.joined()) {
            main_exit = Some(receipt);
        } else {
            assert!(sdk.matches_joined(receipt.joined()));
            sdk_exit = Some(receipt);
        }
    }
    let main_exit = main_exit.unwrap();
    let sdk_exit = sdk_exit.unwrap();
    assert!(main_exit.result().is_ok());
    scope.acknowledge(&main_exit).unwrap();
    let retained_failure = sdk_exit.result().unwrap_err(); // exact error retained BEFORE acceptance/ACK
    sdk.acknowledge_hosted_failure(sdk_exit.joined()).unwrap();
    assert!(Arc::ptr_eq(
        &retained_failure,
        &sdk.try_joined().unwrap().result().unwrap_err()
    ));
    assert!(!sdk_exit.joined().registration_released());
    let mut closing = Box::pin(owner.shutdown(end()));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 1);
    assert!(owner.memory.test_session_metadata_retained());
    assert!(!sdk_exit.joined().registration_released());
    scope.acknowledge(&sdk_exit).unwrap();
    sdk.wait_registration_released().await;
    let report = owner.shutdown(end()).await;
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 0);
    assert!(!owner.memory.test_session_metadata_retained());
    assert!(Arc::ptr_eq(
        &retained_failure,
        &sdk.try_joined().unwrap().result().unwrap_err()
    ));
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}

#[tokio::test]
#[ignore = "requires elevated Windows, wintun.dll and permission to create/configure a real adapter"]
async fn actual_session_end_and_same_sdk_ack_return_requested_capacity_and_session_header() {
    let (mut scope, roots) = scope(2);
    let owner = owner(&scope, roots.clone(), "ZeroRingReceiptTest".into());
    let (send, input) = oneshot::channel::<GrantedPacketDevice>();
    let (reading, ready) = oneshot::channel();
    let stop = owner.cancel.clone();
    let (_,main)=scope.registrar().spawn_tracked(TaskClass::Critical,async move {
        let mut grant=input.await.map_err(|source|error(ComponentId(197),Reason::Revoked,"same native view bootstrap").with_source(source))?;
        let mut reading=Some(reading);let mut bytes=[0;65536];
        loop {
            let receive=grant.io.read(&mut bytes);tokio::pin!(receive);
            let size=tokio::select! {
                _=stop.cancelled()=>return Ok(()),
                size=std::future::poll_fn(|cx| {
                    let result=receive.as_mut().poll(cx);
                    if result.is_pending() { if let Some(reading)=reading.take() { let _=reading.send(()); } }
                    result
                })=>size.map_err(|source|error(ComponentId(197),Reason::Io,"actual original Wintun read").with_source(source))?,
            };
            if size==0 { return Ok(()); }
        }
    }).unwrap();
    let grant = tokio::time::timeout(Duration::from_secs(30), owner.open(main.clone()))
        .await
        .unwrap()
        .unwrap();
    let native = grant.original.clone();
    send.send(grant)
        .unwrap_or_else(|_| panic!("actual driver must retain its original view"));
    tokio::time::timeout_at(end(), ready)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 1);
    assert_eq!(
        roots.packet.requested_rings().requested.occupied.bytes,
        64 * 1024 * 1024
    );
    assert!(owner.memory.test_session_metadata_retained());
    assert!(native.closed().is_none());
    owner.cancel.cancel();
    scope.begin_shutdown();
    let (report, ()) = tokio::join!(owner.shutdown(end()), async {
        for _ in 0..2 {
            let actual = tokio::time::timeout_at(end(), scope.next_receipt())
                .await
                .unwrap()
                .unwrap();
            assert!(actual.result().is_ok());
            scope.acknowledge(&actual).unwrap();
        }
    });
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(owner.evidence().physical_confirmed());
    assert!(owner.original.ring_request_finished());
    assert!(owner.original.session_allocation_returned());
    assert!(native.closed().unwrap().matches(&native));
    assert_eq!(roots.packet.requested_rings().requested.occupied.frames, 0);
    assert!(!owner.memory.test_session_metadata_retained());
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert!(scope.next_receipt().await.is_none());
}
