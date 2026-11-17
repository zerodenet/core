//! An ordinary Source receives only its real initializer's native rejection.
use super::*;

#[tokio::test]
async fn ordinary_source_retains_native_rejection_without_losing_original_close_or_task_failure() {
    timeout(Duration::from_secs(5), async {
        for panic_after_rejection in [false, true] {
            let mut fixture = Fixture::cold(1).await;
            let mut scope = TaskScope::new(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1).unwrap(),
            );
            let fence = Arc::new(CancellationToken::new());
            let binding = fixture.transport.binding().clone();
            let bound = fixture
                .owner
                .registration()
                .bind_declared(
                    &crate::contract::component::ResourceDeclaration::ControlledDirectStream {
                        dial: binding.dial.clone(),
                        max_sockets: NonZeroUsize::new(1).unwrap(),
                    },
                    &scope.registrar(),
                    &fence,
                )
                .unwrap();
            fixture.owner.start(deadline()).await.unwrap();
            let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let target = Destination::new(
                Address::Ipv4([255, 255, 255, 255]),
                held.local_addr().unwrap().port(),
            );
            let mut grant = fixture.grants.reserve().unwrap();
            let adoption = grant.adoption();
            let (view, preparing) = grant.claim_preparing_transferred_view().unwrap();
            let producer = preparing.clone();
            let (send, receive) =
                tokio::sync::oneshot::channel::<crate::contract::scope::TaskJoinWitness>();
            let selection = fixture.egress.clone();
            let (_, witness) = adoption
                .spawn_auxiliary(&scope.registrar(), async move {
                    let actual = receive.await.unwrap();
                    let call = context();
                    let ticket = bound
                        .prepared_direct_service_issuer(
                            &fence,
                            &grant,
                            actual.clone(),
                            &target,
                            &binding,
                        )
                        .unwrap()
                        .authorize(call.cancel.clone(), call.deadline)
                        .unwrap();
                    let native = NativeCarrierOpener::service(ticket, selection, None).unwrap();
                    let source = Arc::new(
                        native
                            .prepare_controlled_stream(&binding, target, call, grant)
                            .unwrap()
                            .open(deadline())
                            .await
                            .err()
                            .expect("actual kernel connect rejection"),
                    );
                    assert!(crate::controlled_carrier::original_connect_error(&source).is_some());
                    producer
                        .retain_connect_rejection_result(&actual, source.clone())
                        .unwrap();
                    if panic_after_rejection {
                        panic!("actual panic after native operation rejection");
                    }
                    Err(ChannelError::retained(source))
                })
                .unwrap();
            send.send(witness.clone()).unwrap();
            let exit = scope.next_exit().await.unwrap();
            let joined = witness.try_joined().unwrap();
            assert!(joined.result().is_err() && joined.registration_released());
            assert!(!joined.failure_accepted());
            let received = adoption
                .receive_ordinary_connect_rejection(&scope, &exit)
                .unwrap();
            assert_eq!(received.is_some(), !panic_after_rejection);
            assert_eq!(joined.failure_accepted(), !panic_after_rejection);
            assert!(adoption.closed().is_none(), "delivery is not native close");
            assert!(
                fixture.grants.reserve().is_err(),
                "delivery is not original capacity return"
            );
            drop(view);
            let report = adoption.retire(deadline()).await.unwrap();
            assert!(report.is_complete(), "{report:?}");
            assert!(report.exited_tasks.contains(&exit.id));
            assert_eq!(report.failed_tasks.is_empty(), !panic_after_rejection);
            assert!(adoption.closed().unwrap().matches(&adoption));
            assert!(fixture
                .owner
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
            assert!(scope.shutdown(deadline()).await.is_complete());
        }
    })
    .await
    .unwrap();
}
