//! One accepted original returns global credit after its own real HostACK.
use super::*;
use crate::contract::scope::{TaskClass, TaskScope};
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
        original
            .bind_original_totals(super::totals::instance(1), &scope.registrar(), &roots)
            .unwrap();
        let mut listener = original.bind_listener(address).unwrap();
        let address = listener.local_addr().unwrap();
        let receiver = original.accepted_task_receiver().unwrap();
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
            scope.acknowledge(&receipt).unwrap();
            assert_eq!(
                roots.snapshot().charged,
                1,
                "same live Source can reuse actual released global capacity"
            );
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
    })
    .await
    .unwrap();
}
