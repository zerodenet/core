//! Cancellation before handoff drops the same staged IO under Root cleanup.
use super::*;
#[tokio::test]
async fn cancelled_staged_original_closes_without_registering_a_job_or_waiting_for_another_dial() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        fixture
            .owner
            .registration()
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
        let transport = fixture.transport.clone();
        let destination = fixture.destination.clone();
        let (returned, ready) = tokio::sync::oneshot::channel();
        let (_, auxiliary) = original
            .spawn_auxiliary(&tasks, async move {
                let carrier = transport
                    .prepare_controlled_stream(destination, context(), grant)?
                    .open(deadline())
                    .await?;
                let (io, _) = carrier.into_parts();
                let job = preparation.reserve_job()?;
                job.stage(
                    Duration::ZERO,
                    Box::pin(async move {
                        let _original_io = io;
                        let _actual_view = view;
                        std::future::pending::<Result<(), ChannelError>>().await
                    }),
                )?;
                assert!(returned.send(job).is_ok());
                Err(error(
                    instance(),
                    Stage::Handshake,
                    Reason::Io,
                    DeliveryState::NotSubmitted,
                )
                .with_source(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "actual rejected preparation",
                )))
            })
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let job = ready.await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(auxiliary.matches_joined(receipt.joined()));
        let received = original.accept_auxiliary().await.unwrap().unwrap();
        assert!(received.failure_accepted() && !received.registration_released());
        assert!(original.take_preparation_job(&tasks, &received).is_err());
        assert!(original.retained_preparation_job().is_none());
        assert_eq!(totals.preparation_snapshot().jobs, 1);
        original.request_retirement();
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
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(original.closed().unwrap().driver_exit().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        scope.acknowledge(&receipt).unwrap();
        auxiliary.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(fixture.owner.retained(), 0);
        assert_eq!(totals.preparation_snapshot().jobs, 0);
        assert!(job.request().await.is_err());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
