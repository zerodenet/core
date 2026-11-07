//! Real original IO, sequential N1 tasks and independently deferred Host ACK.
use super::*;
use crate::contract::component::ResourceDeclaration;
use std::task::Poll;

#[path = "preparation_jobs/budget.rs"]
mod budget;
#[path = "preparation_jobs/unsealed.rs"]
mod unsealed;

#[tokio::test]
async fn rejected_original_job_keeps_native_and_n1_until_actual_job_ack_without_another_dial() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
        totals
            .install_preparation_limits_after_retirement(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let fence = Arc::new(CancellationToken::new());
        let registration = fixture.owner.registration();
        registration
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: fixture.destination.clone(),
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &tasks,
                &fence,
                &totals,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let original = grant.adoption();
        let (view, preparation) = grant.claim_preparing_transferred_view().unwrap();
        assert!(
            preparation.reserve_job().is_err(),
            "no actual Aux/acquisition yet"
        );
        let source = Arc::new(
            error(
                instance(),
                Stage::Handshake,
                Reason::Io,
                DeliveryState::NotSubmitted,
            )
            .with_source(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "original proxy authentication rejection",
            )),
        );
        let rejected = source.clone();
        let transport = fixture.transport.clone();
        let destination = fixture.destination.clone();
        let call = context();
        let first_borrower = call.cancel.clone();
        let (returned, ready) = tokio::sync::oneshot::channel();
        let (_, auxiliary) = original
            .spawn_auxiliary(&tasks, async move {
                let carrier = transport
                    .prepare_controlled_stream(destination, call, grant)?
                    .open(deadline())
                    .await?;
                let (io, _) = carrier.into_parts();
                let (mut io, input) = io.into_driver().split(|io| (io, ()));
                let job = preparation.reserve_job()?;
                assert!(job.request().await.is_err(), "staging is not a sealed job");
                let requests = job.clone();
                job.stage(
                    Duration::ZERO,
                    Box::pin(async move {
                        let _view = view;
                        let request = requests.request().await?;
                        assert_eq!(request.body_payload_limit(), 4 * 1024 * 1024);
                        io.write_all(b"navigation").await.map_err(|cause| {
                            error(instance(), Stage::Send, Reason::Io, DeliveryState::Unknown)
                                .with_source(cause)
                        })?;
                        io.flush().await.map_err(|cause| {
                            error(instance(), Stage::Send, Reason::Io, DeliveryState::Unknown)
                                .with_source(cause)
                        })?;
                        let mut reply = [0; 5];
                        io.read_exact(&mut reply).await.map_err(|cause| {
                            error(
                                instance(),
                                Stage::Receive,
                                Reason::Io,
                                DeliveryState::Unknown,
                            )
                            .with_source(cause)
                        })?;
                        assert_eq!(&reply, b"reply");
                        drop(request);
                        Ok(())
                    }),
                )?;
                assert!(returned.send((input, job)).is_ok());
                Err(ChannelError::retained(rejected))
            })
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (input, job) = ready.await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(auxiliary.matches_joined(receipt.joined()));
        let received = original.accept_auxiliary().await.unwrap().unwrap();
        assert!(received.result().is_err() && received.failure_accepted());
        assert!(!received.registration_released());
        assert!(original.take_preparation_job(&tasks, &received).is_err());
        assert!(original.retained_preparation_job().is_none());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        scope.acknowledge(&receipt).unwrap();
        let released = auxiliary.wait_registration_released().await;
        assert!(auxiliary.matches_joined(&released) && released.registration_released());
        let mut foreign_scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert!(
            original
                .take_preparation_job(&foreign_scope.registrar(), &received)
                .is_err(),
            "same rejected receipt cannot transfer IO to a foreign Scope"
        );
        let staged = original
            .take_preparation_job(&tasks, &received)
            .unwrap()
            .unwrap();
        assert!(
            !staged
                .grant
                .matches_preparation(&original.preparation().unwrap()),
            "the actual setup has ended; it cannot stage additional IO"
        );
        staged.grant.accept_rejection(source, &received).unwrap();
        let work = staged.work;
        let (_, driver) = original
            .spawn_preparation_job(
                &tasks,
                &staged.grant,
                input,
                move |_| async move { work.await },
            )
            .unwrap();
        let proof = original
            .retained_preparation_job()
            .expect("sealed actual job owns IO");
        assert!(proof.retains(&original));
        assert!(proof.driver().same_registration(&driver));
        assert!(proof.owner().unwrap().same_owner(&registration));
        assert!(
            original.retained_driver().is_none(),
            "a rejected job is never a cached pool"
        );
        let mut request = [0; 10];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"navigation");
        first_borrower.cancel();
        assert!(
            !driver.is_joined(),
            "first borrower cancellation does not retire a sealed job"
        );
        let charged = totals.preparation_snapshot();
        assert_eq!((charged.jobs, charged.requests), (1, 1));
        assert!(timeout(Duration::from_millis(20), job.request())
            .await
            .is_err());
        assert!(totals
            .install_preparation_limits_after_retirement(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .is_err());
        peer.write_all(b"reply").await.unwrap();
        let job_receipt = scope.next_receipt().await.unwrap();
        assert!(driver.matches_joined(job_receipt.joined()));
        assert!(job_receipt.result().is_ok());
        assert!(!job_receipt.joined().registration_released());
        // This is the same production Root polling hook. No extra task,
        // another acquire or explicit owner shutdown can make progress for it.
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
        assert_eq!(totals.preparation_snapshot().jobs, 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert_eq!(fixture.owner.retained(), 1);
        scope.acknowledge(&job_receipt).unwrap();
        let released = driver.wait_registration_released().await;
        assert!(driver.matches_joined(&released) && released.registration_released());
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
        assert_eq!(totals.preparation_snapshot().requests, 0);
        assert_eq!(fixture.owner.retained(), 0);
        let next = fixture.grants.reserve().unwrap();
        assert!(
            !proof.retains(&next.adoption()),
            "an old job receipt cannot retain another original of the same owner"
        );
        drop(next);
        assert!(foreign_scope.shutdown(deadline()).await.is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        assert_eq!(totals.snapshot().charged, 0);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

#[path = "preparation_jobs/polling.rs"]
mod polling;
