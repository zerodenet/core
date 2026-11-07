//! The same actual Source metadata native/main/probes and issuer/receiver book.
use super::*;
use crate::{
    contract::scope::{TaskClass, TaskScope},
    controlled_carrier::CarrierOriginalTotals,
};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
#[tokio::test]
async fn source_metadata_actual_wire_native_and_user_receipt_precede_real_scope_ack_refund() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(bound(1));
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(1)));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let owner = Arc::new(Mutex::new(MetadataOwner::new(limits()).unwrap()));
        let users = Arc::new(
            crate::controlled_datagram::metadata::source_totals::MetadataSourceUsers::default(),
        );
        let totals = crate::controlled_datagram::metadata::source_totals::MetadataSourceTotals::new(
            &root,
            ComponentInstanceId {
                component: ComponentId(62),
                generation: ResourceGeneration(1),
            },
            tasks.retirement_observation(),
            tasks.cancellation(),
            owner.clone(),
            users.clone(),
        );
        totals.reserve().unwrap();
        let GrantedMetadataDatagram {
            mut rx,
            tx,
            mut control,
        } = owner.lock().unwrap().bind(address()).unwrap();
        let local = tx.local_addr().unwrap();
        let (issuer, claim) = control.claim_views(tasks.scope_identity()).unwrap();
        let index = users.retain(issuer);
        users.transferred(index);
        let (graph, mut receiver) = claim
            .bind_users::<u8, ()>(tasks.clone(), bound(1), bound(1))
            .ok()
            .unwrap();
        let completion = graph.completion();
        let logical = graph.users().acquire().unwrap();
        let (notice, pending) = tokio::sync::oneshot::channel();
        let id = graph
            .drivers()
            .spawn(1, TaskClass::Auxiliary, |body| async move {
                let mut bytes = [0; 64];
                let mut buffers = [IoSliceMut::new(&mut bytes)];
                let mut metadata = [MetadataReceive::default()];
                let mut notice = Some(notice);
                let count = poll_fn(|cx| {
                    let result = rx.poll_recv(cx, &mut buffers, &mut metadata);
                    if result.is_pending() {
                        if let Some(notice) = notice.take() {
                            notice.send(()).unwrap();
                        }
                    }
                    result
                })
                .await
                .unwrap();
                assert_eq!(count, 1);
                assert_eq!(&buffers[0][..metadata[0].len], b"same Source original");
                body.record(());
                drop((rx, tx));
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        assert_eq!(
            root.snapshot().charged,
            1,
            "main and every real capability probe remain one actual owner custody"
        );
        let peer = tokio::net::UdpSocket::bind(address()).await.unwrap();
        peer.send_to(b"same Source original", local).await.unwrap();
        let joined = scope.next_receipt().await.unwrap();
        joined.result().unwrap();
        graph.drivers().revoke_registration();
        graph.users().cancellation().cancel();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        graph.drivers().wait_joined(deadline).await.unwrap();
        receiver.receive(id, deadline).await.unwrap();
        assert!(
            completion.try_closed().is_none(),
            "last logical guard is not a native view"
        );
        let native = control.close().unwrap();
        assert_eq!(native.reservation(), control.reservation());
        assert_eq!(root.snapshot().charged, 1);
        drop(logical);
        let receipt = completion.try_closed().unwrap();
        assert!(users.confirm(&receipt, &tasks));
        assert_eq!(users.pending(), 0);
        assert_eq!(users.confirmed().len(), 1);
        assert_eq!(
            root.snapshot().charged,
            1,
            "native and received user graph are independent of Host ACK"
        );
        scope.acknowledge(&joined).unwrap();
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert_eq!(root.snapshot().charged, 0);
        let report = owner.lock().unwrap().close_all();
        assert!(report.is_complete());
        assert_eq!(
            report.main.confirmed_closed, 1,
            "Root did not consume the Source native report"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn source_root_native_retirement_invokes_original_io_wakers_outside_metadata_owner_mutex() {
    use std::task::{Wake, Waker};
    struct Reenter {
        owner: Arc<Mutex<MetadataOwner>>,
        wakes: std::sync::atomic::AtomicUsize,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(
                self.owner.try_lock().is_ok(),
                "actual native callback reentered a held MetadataOwner"
            );
            self.wakes.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
    let root = CarrierOriginalTotals::new(bound(1));
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let stop = crate::contract::CancellationToken::new();
    let owner = Arc::new(Mutex::new(MetadataOwner::new(limits()).unwrap()));
    let users = Arc::new(
        crate::controlled_datagram::metadata::source_totals::MetadataSourceUsers::default(),
    );
    let totals = crate::controlled_datagram::metadata::source_totals::MetadataSourceTotals::new(
        &root,
        ComponentInstanceId {
            component: ComponentId(64),
            generation: ResourceGeneration(1),
        },
        scope.registrar().retirement_observation(),
        stop.clone(),
        owner.clone(),
        users,
    );
    totals.reserve().unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        mut control,
    } = owner.lock().unwrap().bind(address()).unwrap();
    let callbacks = Arc::new(Reenter {
        owner: owner.clone(),
        wakes: std::sync::atomic::AtomicUsize::new(0),
    });
    let reenter = callbacks.clone();
    let (notice, pending) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let mut bytes = [0; 1];
            let mut buffers = [IoSliceMut::new(&mut bytes)];
            let mut metadata = [MetadataReceive::default()];
            let waker = Waker::from(reenter);
            assert!(
                rx.poll_recv(
                    &mut Context::from_waker(&waker),
                    &mut buffers,
                    &mut metadata
                )
                .is_pending()
            );
            notice.send(()).unwrap();
            released.await.unwrap();
            assert!(
                poll_fn(|cx| rx.poll_recv(cx, &mut buffers, &mut metadata))
                    .await
                    .is_err()
            );
            drop((rx, tx));
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    stop.cancel();
    poll_fn(|cx| {
        root.poll_cancelled_retirements(cx);
        Poll::Ready(())
    })
    .await;
    assert!(callbacks.wakes.load(std::sync::atomic::Ordering::Acquire) > 0);
    assert_eq!(
        root.snapshot().charged,
        1,
        "the real Source caller and IO views remain"
    );
    let native = control.close().unwrap();
    assert_eq!(native.reservation(), control.reservation());
    release.send(()).unwrap();
    assert!(
        scope
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(3))
            .await
            .is_complete()
    );
    assert_eq!(root.snapshot().charged, 0);
    let report = owner.lock().unwrap().take_report();
    assert_eq!(
        report.main.confirmed_closed, 1,
        "Root snapshot did not steal Source report delta"
    );
}
