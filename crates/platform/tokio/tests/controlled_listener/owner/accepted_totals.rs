//! One accepted original returns global credit after its own real HostACK.
use super::*;
use crate::contract::{
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Stage,
};
#[tokio::test]
async fn same_live_source_accepts_again_only_after_original_native_users_and_host_ack() {
    timeout(Duration::from_secs(5), async {
        let roots =
            crate::controlled_carrier::CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let address = "127.0.0.1:0".parse().unwrap();
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration(address, 1),
            NonZeroUsize::new(1).unwrap(),
            CancellationToken::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            &scope.registrar(),
        )
        .unwrap();
        let instance = super::totals::instance(1);
        // This explicit library fixture owns its metadata Root. It is never a
        // fallback for missing production Host binding.
        let packet_totals = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let failures = crate::AcceptedTaskFailures::prepare(
            instance,
            &scope.registrar(),
            &packet_totals.driver_metadata_view(),
        )
        .unwrap();
        original.bind_task_failure_receiver(&failures).unwrap();
        original
            .bind_original_totals(instance, &scope.registrar(), &roots)
            .unwrap();
        let mut listener = original.bind_listener(address).unwrap();
        let address = listener.local_addr().unwrap();
        let receiver = original.accepted_task_receiver().unwrap();
        let mut retained_origin = None;
        for index in 0..4_u8 {
            {
                let mut waiting = Box::pin(listener.accept_owned());
                poll_fn(|cx| {
                    assert!(waiting.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert_eq!(
                    roots.snapshot().charged,
                    1,
                    "known native Pending only retains listener"
                );
            }
            let (client, accepted) =
                tokio::join!(TcpStream::connect(address), listener.accept_owned());
            let mut client = client.unwrap();
            let (accepted, _) = accepted.unwrap();
            let claim = accepted.claim();
            let lineage = receiver.carrier_origin(&claim).unwrap();
            assert_eq!(lineage.instance(), instance);
            assert!(lineage.scope_identity().same_scope(&scope.registrar().scope_identity()));
            assert!(!lineage.is_settled());
            let origin = crate::controlled_carrier::CarrierOrigin::from_accepted(lineage.clone());
            assert!(origin.accepted().unwrap().same_original(&lineage));
            assert!(origin.closed().is_none());
            assert_eq!(
                roots.snapshot().charged,
                2,
                "before handler firstawait, same accepted original is charged"
            );
            let (ready, waiting) = tokio::sync::oneshot::channel();
            let (id, witness) = accepted
                .spawn_handler(
                    &scope.registrar(),
                    TaskClass::Auxiliary,
                    move |mut io, _| async move {
                        let mut byte = [0];
                        {
                            let mut read = Box::pin(io.read_exact(&mut byte));
                            poll_fn(|cx| {
                                assert!(read.as_mut().poll(cx).is_pending());
                                Poll::Ready(())
                            })
                            .await;
                            ready.send(()).unwrap();
                            read.await.map_err(|source| {
                                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                                    .with_source(source)
                            })?;
                        }
                        io.write_all(&byte).await.map_err(|source| {
                            ChannelError::new(Stage::Send, Reason::Io, Impact::Session)
                                .with_source(source)
                        })?;
                        io.flush().await.map_err(|source| {
                            ChannelError::new(Stage::Send, Reason::Io, Impact::Session)
                                .with_source(source)
                        })?;
                        drop(io);
                        Ok(())
                    },
                )
                .unwrap();
            waiting.await.unwrap();
            client.write_all(&[index]).await.unwrap();
            let mut echoed = [0];
            client.read_exact(&mut echoed).await.unwrap();
            assert_eq!(echoed, [index]);
            let receipt = scope.next_receipt().await.unwrap();
            assert_eq!(receipt.id(), id);
            assert!(witness.matches_joined(receipt.joined()));
            assert!(receiver.observe_joined(id).unwrap().result().is_ok());
            let closed = receiver.close(&claim, deadline()).await.unwrap().unwrap();
            assert!(closed.matches(&claim));
            assert_eq!(
                roots.snapshot().charged,
                2,
                "native+unique view+graph joins are independent of real HostACK"
            );
            assert!(!witness.try_joined().unwrap().registration_released());
            assert_eq!(original.accepted_evidence().unwrap().retained,1,"local original row is still charged before actual HostACK");
            {
                let mut unacknowledged=Box::pin(listener.accept_owned_wait());
                poll_fn(|cx|{assert!(unacknowledged.as_mut().poll(cx).is_pending(),"ordinary Source waits without accepting a new native before old original ACK");Poll::Ready(())}).await;
            }
            assert_eq!(original.accepted_evidence().unwrap().retained,1,"cancelling the waiter cannot release the original row");
            assert_eq!(roots.snapshot().charged,2);
            assert!(lineage.native_closed().unwrap().matches(&claim));
            assert!(lineage.closed().is_none(), "native positive does not acknowledge the original registration");
            assert!(!lineage.is_settled());
            let mut global_return = Box::pin(lineage.wait_settled_original());
            struct Reentrant {
                root: std::sync::Weak<crate::controlled_carrier::CarrierOriginalTotals>,
                wakes: std::sync::atomic::AtomicUsize,
            }
            impl std::task::Wake for Reentrant {
                fn wake(self: Arc<Self>) { Self::wake_by_ref(&self); }
                fn wake_by_ref(this: &Arc<Self>) {
                    assert_eq!(this.root.upgrade().unwrap().snapshot().charged, 1,
                        "global removal precedes notification, outside the Root gate");
                    this.wakes.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
            let returned = Arc::new(Reentrant { root: Arc::downgrade(&roots), wakes: Default::default() });
            let waker = std::task::Waker::from(returned.clone());
            assert!(global_return.as_mut().poll(&mut std::task::Context::from_waker(&waker)).is_pending());
            assert_eq!(returned.wakes.load(std::sync::atomic::Ordering::Acquire), 0);
            scope.acknowledge(&receipt).unwrap();
            assert!(returned.wakes.load(std::sync::atomic::Ordering::Acquire) > 0);
            global_return.await.unwrap();
            assert!(lineage.is_settled());
            assert!(lineage.closed().unwrap().matches(&claim));
            assert!(origin.closed().unwrap().matches(&origin));
            retained_origin = Some(origin);
            assert_eq!(
                roots.snapshot().charged,
                1,
                "same live Source can reuse actual released global capacity"
            );
            assert_eq!(original.accepted_evidence().unwrap().retained,0,"the same exact ACK releases the local row as well as global custody");
            assert!(!scope.is_drained() && scope.registrar().scope_identity().registration_open());
            assert_eq!(client.read(&mut echoed).await.unwrap(), 0);
        }
        assert_eq!(
            original
                .accepted_evidence()
                .unwrap()
                .confirmed_native_closes,
            4
        );
        assert!(
            original
                .wait_closed(deadline())
                .await
                .unwrap()
                .is_complete()
        );
        assert_eq!(original.confirmed_close_count(), 1);
        scope.begin_shutdown();
        assert!(scope.is_drained());
        assert_eq!(roots.snapshot().charged, 1);
        drop(listener);
        assert_eq!(roots.snapshot().charged, 0);
        let root_weak = Arc::downgrade(&roots);
        drop(roots);
        assert!(root_weak.upgrade().is_none(), "strong Accepted token cannot retain the Host Root through a parent ledger");
        assert!(retained_origin.unwrap().accepted().unwrap().is_settled(), "only the earlier actual Root removal remains positive");
    })
    .await
    .unwrap();
}
