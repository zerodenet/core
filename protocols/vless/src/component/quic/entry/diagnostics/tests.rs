//! Same establishing Aux: real Pending -> panic, compared with normal stop.
use super::*;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
    time::Duration,
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    contract::{component::ResourceDeclaration, scope::TaskScope, CancellationToken},
    controlled_carrier::CarrierOriginalTotals,
    controlled_datagram::{metadata::MetadataLimits, metadata_pool::MetadataPoolOwner},
    packet_memory::{PacketMetadataLayout, PacketReturnTotals},
};
#[tokio::test]
async fn actual_pending_initializer_panic_is_retained_before_local_acceptance_and_root_return() {
    actual(true).await;
    actual(false).await;
}
async fn actual(panic: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let totals = CarrierOriginalTotals::new(one);
        let memory = Arc::new(PacketReturnTotals::default()).driver_metadata_view();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let instance = ComponentInstanceId {
            component: ComponentId(901),
            generation: ResourceGeneration(3),
        };
        let limits = MetadataLimits {
            main_sockets: one,
            probe_sockets: one,
            probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
            concurrent_send_calls: one,
            write_pollers: one,
        };
        let pool = MetadataPoolOwner::new(instance, one, limits, &host).unwrap();
        pool.registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataCarrier {
                    endpoint: Destination::new(Address::Ipv4([127, 0, 0, 1]), 443),
                    dial: Default::default(),
                    ports: vec![],
                    limits,
                    max_origins: one,
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        pool.start().unwrap();
        let diagnostics = Diagnostics::prepare(&memory).unwrap();
        let received = memory
            .reserve(
                PacketMetadataLayout::array::<zero_transport::quic::owned::DriverDelivery>(0)
                    .unwrap(),
            )
            .unwrap();
        let logical = memory
            .reserve(PacketMetadataLayout::boxed::<super::super::LogicalControl>())
            .unwrap();
        let owners = PacketMetadataArc::from_prepaid(
            Owners::new(tasks.clone(), diagnostics, received, logical, 0),
            memory
                .reserve(PacketMetadataArc::<Owners>::layout().unwrap())
                .unwrap(),
        )
        .unwrap_or_else(|_| panic!("paid original Owners"));
        let (prepared, retirement) = pool
            .grants()
            .prepare_protocol_original(
                &tasks,
                Box::new(super::super::ProtocolControl(owners.clone())).into(),
            )
            .unwrap_or_else(|source| panic!("{:?}", source.error));
        let origin = prepared.origin();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let release = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(tokio::sync::Notify::new());
        let gate = release.clone();
        let notify = changed.clone();
        let stop = owners.stop.clone();
        let (permit, actor) = prepared
            .spawn_auxiliary(&tasks, async move {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let mut entered = Some(entered);
                let pending = std::future::poll_fn(|cx| {
                    let result = if gate.load(Ordering::Acquire) {
                        Poll::Ready(())
                    } else {
                        std::future::Future::poll(notified.as_mut(), cx)
                    };
                    if result.is_pending() {
                        if let Some(entered) = entered.take() {
                            let _ = entered.send(());
                        }
                    }
                    result
                });
                tokio::select! {biased;
                    _=stop.cancelled()=>return Ok(()),
                    _=pending=>{},
                }
                assert!(!panic, "actual pending QUIC initializer panic");
                Ok(())
            })
            .unwrap();
        owners.set_actor(Some(actor.clone()));
        waiting.await.unwrap();
        assert!(actor.try_joined().is_none());
        assert_eq!(totals.snapshot().charged, 1);
        if panic {
            release.store(true, Ordering::Release);
            changed.notify_waiters();
        } else {
            owners.stop.cancel();
        }
        let receipt = scope.next_receipt().await.unwrap();
        assert!(actor.matches_joined(receipt.joined()));
        let source = receipt.result().err();
        assert_eq!(source.is_some(), panic);
        owners.receive_actor_original().await.unwrap();
        assert_eq!(receipt.local_failure_accepted(), panic);
        if let Some(source) = &source {
            assert!(Arc::ptr_eq(
                owners.diagnostics.payload.terminal.get().unwrap(),
                source
            ));
            assert!(owners.diagnostics.payload.operation.get().is_none());
            let pending = owners
                .diagnostics
                .pending_failure()
                .expect("sole real TaskJoined panic is independently reportable");
            assert!(pending.cleanup_failure_receipt().is_some());
        } else {
            assert!(owners.diagnostics.pending_failure().is_none());
        }
        retirement.request_retirement();
        let fault = if panic {
            Some(
                std::future::poll_fn(|cx| {
                    totals.poll_cancelled_retirements(cx);
                    match totals.resource_failures().into_iter().next() {
                        Some(fault) => Poll::Ready(fault),
                        None => Poll::Pending,
                    }
                })
                .await,
            )
        } else {
            None
        };
        if let Some(fault) = &fault {
            assert!(!fault.ownership_received());
            assert!(fault.acknowledge_retained(fault));
            // The retained pure capsule still owns the exact original panic Arc.
            assert!(Arc::ptr_eq(
                owners.diagnostics.payload.terminal.get().unwrap(),
                source.as_ref().unwrap()
            ));
        }
        assert!(!receipt.joined().registration_released());
        assert!(!retirement.is_settled());
        assert_eq!(totals.snapshot().charged, 1);
        scope.acknowledge(&receipt).unwrap();
        actor.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(retirement.is_settled());
        let closed = origin.closed().unwrap();
        assert!(closed.matches(&origin) && !closed.had_native_acquisition());
        drop(permit);
        // Same N1 is usable again after actual fault receipt, native no-acquire
        // proof and the original Scope ACK, without increasing any capacity.
        let next = pool.grants().prepare_original(&tasks).unwrap();
        drop(next);
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        if let Some(fault) = fault {
            assert!(fault.ownership_received());
            assert!(Arc::ptr_eq(
                owners.diagnostics.payload.terminal.get().unwrap(),
                source.as_ref().unwrap()
            ));
        }
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
