use super::*;
#[tokio::test]
async fn controlled_carrier_real_driver_exit_is_not_join_and_join_is_not_original_fd_close() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let entered = Arc::new(tokio::sync::Notify::new());
        let entering = entered.clone();
        let (id, witness) = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Critical,
                io.into_driver(),
                |mut io| async move {
                    let mut bytes = [0; 1];
                    let source = std::future::poll_fn(|cx| {
                        let mut buffer = tokio::io::ReadBuf::new(&mut bytes);
                        let result = tokio::io::AsyncRead::poll_read(
                            std::pin::Pin::new(&mut io),
                            cx,
                            &mut buffer,
                        );
                        if result.is_pending() {
                            entering.notify_one();
                        }
                        result
                    })
                    .await
                    .err()
                    .unwrap();
                    Err(
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                            .with_source(source),
                    )
                },
            )
            .unwrap();
        entered.notified().await;
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        {
            use std::future::Future;
            let mut closing = Box::pin(adoption.retire(deadline()));
            std::future::poll_fn(|cx| {
                assert!(
                    closing.as_mut().poll(cx).is_pending(),
                    "native cleanup waits for actual scope join"
                );
                std::task::Poll::Ready(())
            })
            .await;
        }
        assert!(
            adoption.slot.state().control.is_some(),
            "dropping a pending close preserves the original control"
        );
        let report = adoption
            .retire(Instant::now() + Duration::from_millis(20))
            .await
            .unwrap();
        assert!(!report.is_complete());
        assert!(report
            .unfinished
            .contains(&crate::contract::lifecycle::Unfinished::Task(id)));
        assert!(!witness.is_joined());
        assert!(adoption.closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "body exit/drop did not close original descriptor"
        );
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        let joined = witness.wait().await;
        assert!(joined.result().is_err());
        assert!(
            adoption.closed().is_none(),
            "actual join is still not native close"
        );
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        let report = adoption.retire(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        let receipt = adoption.closed().unwrap();
        assert!(receipt.matches(&adoption));
        assert_eq!(receipt.instance(), instance());
        let original = joined.result().unwrap_err();
        let retained = receipt.driver_exit().unwrap().result().unwrap_err();
        assert!(Arc::ptr_eq(&original, &retained));
        assert_eq!(fixture.owner.retained(), 0);
        drop(fixture.grants.reserve().unwrap());
        assert!(adoption
            .retire(deadline())
            .await
            .unwrap()
            .closed_io
            .is_empty());
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn controlled_carrier_spawn_failure_keeps_control_and_retiring_capacity_until_native_close() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let failure = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Critical,
                io.into_driver(),
                |mut io| async move {
                    let _ = io.read(&mut [0; 1]).await;
                    Ok(())
                },
            )
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::Capacity);
        assert!(adoption.closed().is_none());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(!adoption.retire(Instant::now()).await.unwrap().is_complete());
        assert_eq!(fixture.owner.retained(), 1);
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(fixture.owner.retained(), 0);
        drop(fixture.grants.reserve().unwrap());
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn controlled_carrier_dropped_ready_result_retains_original_and_cannot_forge_other_slot_receipt(
) {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(2).await;
        let (carrier, mut peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        drop(carrier);
        assert!(adoption.closed().is_none());
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        let (other, _other_peer) = fixture.connected().await;
        let other_adoption = other.adoption().clone();
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        let receipt = adoption.closed().unwrap();
        assert!(receipt.matches(&adoption));
        assert!(!receipt.matches(&other_adoption));
        assert!(other_adoption.closed().is_none());
        drop(other);
        assert!(other_adoption
            .retire(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}
