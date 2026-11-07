//! A stopped original is not polled again; an already received actual IO fault
//! remains the same Scope receipt/Arc after later cancellation.
use super::*;
use std::{error::Error as _, future::Future, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
};

#[tokio::test]
async fn actual_native_pending_read_job_stops_before_polling_host_revoked_io() {
    exercise(false).await;
}
#[tokio::test]
async fn actual_peer_eof_job_error_keeps_its_scope_arc_after_late_host_cancellation() {
    exercise(true).await;
}

async fn exercise(peer_failure: bool) {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        // The real native stream token is a child of this exact Host token,
        // so cancelling it would make another TCP poll return closed.
        fixture.owner = CarrierOwner::new(instance(), NonZeroUsize::new(1).unwrap(), &host);
        fixture.grants = fixture.owner.grants();
        let registration = fixture.owner.registration();
        registration
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: fixture.destination.clone(),
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let original = grant.adoption();
        let (view, preparation) = grant.claim_preparing_transferred_view().unwrap();
        let rejection = Arc::new(
            error(
                instance(),
                Stage::Handshake,
                Reason::Io,
                DeliveryState::NotSubmitted,
            )
            .with_source(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "same original rejected proxy handshake",
            )),
        );
        let rejected = rejection.clone();
        let destination = fixture.destination.clone();
        let transport = fixture.transport.clone();
        let (send, prepared) = oneshot::channel();
        let (blocked, pending) = oneshot::channel();
        let (cause, mut fault) = oneshot::channel();
        let (_, auxiliary) = original
            .spawn_auxiliary(&tasks, async move {
                let carrier = transport
                    .prepare_controlled_stream(destination, context(), grant)?
                    .open(deadline())
                    .await?;
                let (io, _) = carrier.into_parts();
                let (mut io, input) = io.into_driver().split(|io| (io, view));
                let job = preparation.reserve_job()?;
                job.stage(
                    Duration::ZERO,
                    Box::pin(async move {
                        let mut bytes = [0; 1];
                        let read = io.read_exact(&mut bytes);
                        tokio::pin!(read);
                        let mut blocked = Some(blocked);
                        let result = std::future::poll_fn(|cx| {
                            let value = read.as_mut().poll(cx);
                            if value.is_pending() {
                                if let Some(blocked) = blocked.take() {
                                    blocked.send(()).unwrap();
                                }
                            }
                            value
                        })
                        .await;
                        let source = result
                            .expect_err("real peer EOF must fail the original pending read_exact");
                        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
                        let source = Arc::new(
                            error(
                                instance(),
                                Stage::Receive,
                                Reason::Io,
                                DeliveryState::Unknown,
                            )
                            .with_source(source),
                        );
                        cause.send(source.clone()).unwrap();
                        Err(ChannelError::retained(source))
                    }),
                )?;
                assert!(send.send((input, job)).is_ok());
                Err(ChannelError::retained(rejected))
            })
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (input, job) = prepared.await.unwrap();
        let setup = scope.next_receipt().await.unwrap();
        assert!(auxiliary.matches_joined(setup.joined()));
        let received = original.accept_auxiliary().await.unwrap().unwrap();
        assert!(received.result().is_err() && received.failure_accepted());
        scope.acknowledge(&setup).unwrap();
        auxiliary.wait_registration_released().await;
        let staged = original
            .take_preparation_job(&tasks, &received)
            .unwrap()
            .unwrap();
        staged.grant.accept_rejection(rejection, &received).unwrap();
        let work = staged.work;
        let (_, driver) = original
            .spawn_preparation_job(&tasks, &staged.grant, input, |view| async move {
                let _view = view;
                work.await
            })
            .unwrap();
        pending
            .await
            .expect("actual original native read must be Pending");
        assert!(!driver.is_joined());
        assert!(original.closed().is_none());
        assert_eq!(totals.preparation_snapshot().jobs, 1);
        if peer_failure {
            peer.shutdown().await.unwrap();
        } else {
            host.cancel();
        }
        let receipt = scope.next_receipt().await.unwrap();
        assert!(driver.matches_joined(receipt.joined()));
        let retained = if peer_failure {
            let actual = fault.await.unwrap();
            let reported = receipt.result().unwrap_err();
            assert_eq!(reported.stage, Stage::Receive);
            let kept = reported
                .source()
                .and_then(|source| source.downcast_ref::<Arc<ChannelError>>())
                .unwrap();
            // Scope retains the wrapper returned by work; it retains the exact
            // original source Arc rather than a reconstructed diagnostic string.
            assert!(Arc::ptr_eq(kept, &actual));
            host.cancel();
            let joined = driver.wait().await;
            assert!(driver.matches_joined(&joined));
            assert!(Arc::ptr_eq(&reported, &joined.result().unwrap_err()));
            assert!(
                !receipt.local_failure_accepted(),
                "reading the real failed receipt cannot itself accept the job"
            );
            Some(reported)
        } else {
            assert!(
                receipt.result().is_ok(),
                "canonical stop must not poll cancelled native IO: {:?}",
                receipt.result()
            );
            assert!(matches!(
                fault.try_recv(),
                Err(oneshot::error::TryRecvError::Closed)
            ));
            None
        };
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if original
                .closed()
                .is_some_and(|closed| closed.matches(&original))
                && original
                    .users_closed()
                    .is_some_and(|closed| closed.matches(&original))
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(totals.preparation_snapshot().jobs, 1);
        assert!(!receipt.joined().registration_released());
        if retained.is_some() {
            assert!(
                receipt.local_failure_accepted(),
                "actual Root receiver retained the real job failure before acceptance"
            );
        }
        scope.acknowledge(&receipt).unwrap();
        let acknowledged = driver.wait_registration_released().await;
        if let Some(retained) = retained {
            assert!(driver.matches_joined(&acknowledged));
            assert!(Arc::ptr_eq(&retained, &acknowledged.result().unwrap_err()));
        }
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(totals.preparation_snapshot().jobs, 0);
        assert!(job.request().await.is_err());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
