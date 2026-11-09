//! Protocol retirement seals the actual job before registration or IO.
use super::*;
use crate::contract::{
    lifecycle::{CloseReport, DynChannelControl},
    BoxFuture,
};
use std::sync::atomic::{AtomicUsize, Ordering};
// This protocol has no separate IO. Native/views and all task/Host receipts
// remain the same Root's independent duties; this report supplies none.
struct EmptyProtocol;
impl DynChannelControl for EmptyProtocol {
    fn cancel(&self, _: crate::contract::CloseReason) {}
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        Ok(Default::default())
    }
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async { Ok(Default::default()) })
    }
}
#[derive(Clone, Copy, Debug)]
enum Cut {
    Reserve,
    Stage,
    Spawn,
    Construct,
    Running,
}
#[tokio::test]
async fn actual_protocol_stop_refuses_late_job_before_reserve_stage_registration_and_seal() {
    timeout(Duration::from_secs(5), async {
        for cut in [
            Cut::Reserve,
            Cut::Stage,
            Cut::Spawn,
            Cut::Construct,
            Cut::Running,
        ] {
            let mut fixture = Fixture::cold(1).await;
            let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
            let mut scope = TaskScope::suspended_hosted(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1),
            );
            scope.enable_registration().unwrap();
            let tasks = scope.registrar();
            let host = Arc::new(CancellationToken::new());
            fixture
                .owner
                .registration()
                .bind_declared_with_totals(
                    &ResourceDeclaration::ControlledCarrierPool {
                        endpoint: fixture.destination.clone(),
                        dial: fixture.transport.binding().dial.clone(),
                        datagram: false,
                        max_sockets: NonZeroUsize::new(1).unwrap(),
                    },
                    &tasks,
                    &host,
                    &totals,
                )
                .unwrap();
            fixture.owner.start(deadline()).await.unwrap();
            let (mut grant, retirement) = fixture
                .grants
                .reserve_stream_protocol_original(&tasks, Box::new(EmptyProtocol).into())
                .unwrap();
            let original = grant.adoption();
            let (view, preparation) = grant.claim_preparing_driver_view().unwrap();
            let transport = fixture.transport.clone();
            let destination = fixture.destination.clone();
            let rejected = Arc::new(
                error(
                    instance(),
                    Stage::Handshake,
                    Reason::Io,
                    DeliveryState::NotSubmitted,
                )
                .with_source(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "actual setup rejection",
                )),
            );
            let source = rejected.clone();
            let phase = retirement.clone();
            let polls = Arc::new(AtomicUsize::new(0));
            let actual_polls = polls.clone();
            let (send, receive) = tokio::sync::oneshot::channel();
            let (_, auxiliary) = original
                .spawn_auxiliary(&tasks, async move {
                    let carrier = transport
                        .prepare_controlled_stream(destination, context(), grant)?
                        .open(deadline())
                        .await?;
                    let (io, _) = carrier.into_parts();
                    let (io, input) = io.into_driver().split(|io| (io, ()));
                    if matches!(cut, Cut::Reserve) {
                        phase.request_retirement();
                        assert!(preparation.reserve_job().is_err());
                        drop(io);
                        drop(view);
                        assert!(send.send(None).is_ok());
                    } else {
                        let job = preparation.reserve_job()?;
                        if matches!(cut, Cut::Stage) {
                            phase.request_retirement();
                        }
                        let staged = job.stage(
                            Duration::ZERO,
                            Box::pin(async move {
                                let mut io = io;
                                let _view = view;
                                actual_polls.fetch_add(1, Ordering::SeqCst);
                                tokio::io::AsyncWriteExt::write_all(&mut io, b"job")
                                    .await
                                    .map_err(|cause| {
                                        error(
                                            instance(),
                                            Stage::Send,
                                            Reason::Io,
                                            DeliveryState::Unknown,
                                        )
                                        .with_source(cause)
                                    })?;
                                std::future::pending::<Result<(), ChannelError>>().await
                            }),
                        );
                        if matches!(cut, Cut::Stage) {
                            assert!(staged.is_err());
                            assert!(send.send(None).is_ok());
                        } else {
                            staged?;
                            assert!(send.send(Some((input, job))).is_ok());
                        }
                    }
                    Err(ChannelError::retained(source))
                })
                .unwrap();
            let (mut peer, _) = fixture.listener.accept().await.unwrap();
            let staged = receive.await.unwrap();
            let receipt = scope.next_receipt().await.unwrap();
            assert!(auxiliary.matches_joined(receipt.joined()));
            let received = original.accept_auxiliary().await.unwrap().unwrap();
            assert!(received.failure_accepted() && !received.registration_released());
            assert_eq!(fixture.owner.retained(), 1);
            assert_eq!(totals.snapshot().charged, 1);
            scope.acknowledge(&receipt).unwrap();
            auxiliary.wait_registration_released().await;
            if let Some((input, job)) = staged {
                let staged = original
                    .take_preparation_job(&tasks, &received)
                    .unwrap()
                    .unwrap();
                staged.grant.accept_rejection(rejected, &received).unwrap();
                let constructs = Arc::new(AtomicUsize::new(0));
                let called = constructs.clone();
                if matches!(cut, Cut::Spawn) {
                    retirement.request_retirement();
                }
                let phase = retirement.clone();
                let work = staged.work;
                let attempted =
                    original.spawn_preparation_job(&tasks, &staged.grant, input, move |_| {
                        called.fetch_add(1, Ordering::SeqCst);
                        if matches!(cut, Cut::Construct) {
                            phase.request_retirement();
                        }
                        async move { work.await }
                    });
                if matches!(cut, Cut::Running) {
                    let (_, driver) = attempted.unwrap();
                    let proof = original
                        .retained_preparation_job()
                        .expect("same original sealed job");
                    assert!(proof.retains(&original) && proof.driver().same_registration(&driver));
                    let mut wire = [0; 3];
                    peer.read_exact(&mut wire).await.unwrap();
                    assert_eq!(&wire, b"job");
                    retirement.request_retirement();
                    let job_receipt = scope.next_receipt().await.unwrap();
                    assert!(driver.matches_joined(job_receipt.joined()));
                    assert!(job_receipt.result().is_ok());
                    assert!(!job_receipt.joined().registration_released());
                    std::future::poll_fn(|cx| {
                        totals.poll_cancelled_retirements(cx);
                        if original
                            .closed()
                            .is_some_and(|closed| closed.matches(&original))
                        {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                    assert_eq!(
                        totals.snapshot().charged,
                        1,
                        "native closure does not replace real driver Host ACK"
                    );
                    assert_eq!(fixture.owner.retained(), 1);
                    scope.acknowledge(&job_receipt).unwrap();
                    driver.wait_registration_released().await;
                } else {
                    assert!(attempted.is_err(), "{cut:?}");
                    assert!(original.slot.state().driver.is_none());
                    assert!(original.retained_preparation_job().is_none());
                }
                assert_eq!(
                    constructs.load(Ordering::SeqCst),
                    usize::from(matches!(cut, Cut::Construct | Cut::Running))
                );
                assert!(job.request().await.is_err());
            }
            assert_eq!(
                polls.load(Ordering::SeqCst),
                usize::from(matches!(cut, Cut::Running))
            );
            std::future::poll_fn(|cx| {
                totals.poll_cancelled_retirements(cx);
                if totals.snapshot().charged == 0 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(original.closed().unwrap().matches(&original));
            assert!(original.closed().unwrap().had_native_acquisition());
            assert!(original.users_closed().unwrap().matches(&original));
            assert!(retirement.is_settled());
            assert_eq!(fixture.owner.retained(), 0);
            assert_eq!(totals.preparation_snapshot().jobs, 0);
            assert!(fixture
                .owner
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
            scope.begin_shutdown();
            assert!(scope.is_drained());
        }
    })
    .await
    .unwrap();
}
