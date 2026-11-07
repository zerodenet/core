//! Two actual owner/Scope instances share the same existing original Root.
use super::*;
use crate::contract::scope::{HostedScope, TaskJoinWitness};

struct JobFixture {
    original: CarrierAdoption,
    fixture: Fixture,
    scope: HostedScope,
    driver: Option<TaskJoinWitness>,
    peer: TcpStream,
    request: tokio::sync::oneshot::Receiver<()>,
    finish: Option<tokio::sync::oneshot::Sender<()>>,
}
impl JobFixture {
    async fn new(
        totals: &Arc<CarrierOriginalTotals>,
        component: ComponentId,
        expect_capacity: bool,
    ) -> Self {
        let mut fixture = Fixture::cold(1).await;
        let actual = ComponentInstanceId {
            component,
            ..instance()
        };
        let host = Arc::new(CancellationToken::new());
        fixture.owner = CarrierOwner::new(actual, NonZeroUsize::new(1).unwrap(), &host);
        fixture.grants = fixture.owner.grants();
        fixture.transport = UpstreamTransport::new(
            fixture.transport.binding().clone(),
            Arc::new(
                NativeCarrierOpener::new(
                    actual,
                    fixture.transport.binding().clone(),
                    fixture.listener.local_addr().unwrap(),
                    fixture.egress.clone(),
                    None,
                )
                .unwrap(),
            ),
        );
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
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
                totals,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let original = grant.adoption();
        let (view, preparation) = grant.claim_preparing_transferred_view().unwrap();
        let rejection = Arc::new(
            error(
                actual,
                Stage::Handshake,
                Reason::Io,
                DeliveryState::NotSubmitted,
            )
            .with_source(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "same original protocol rejection",
            )),
        );
        let rejected = rejection.clone();
        let transport = fixture.transport.clone();
        let destination = fixture.destination.clone();
        let (send, setup) = tokio::sync::oneshot::channel();
        let (requested, request) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (_, auxiliary) = original
            .spawn_auxiliary(&tasks, async move {
                let carrier = transport
                    .prepare_controlled_stream(destination, context(), grant)?
                    .open(deadline())
                    .await?;
                let (io, _) = carrier.into_parts();
                let (mut io, input) = io.into_driver().split(|io| (io, view));
                let job = match preparation.reserve_job() {
                    Ok(job) => {
                        assert!(!expect_capacity, "a third actual owner exceeded Root jobs");
                        job
                    }
                    Err(source) => {
                        assert!(expect_capacity);
                        assert_eq!(source.reason, Reason::Capacity);
                        assert_eq!(source.delivery, DeliveryState::NotSubmitted);
                        assert!(send.send(None).is_ok());
                        return Err(source);
                    }
                };
                let requests = job.clone();
                job.stage(
                    Duration::ZERO,
                    Box::pin(async move {
                        let _request = requests.request().await?;
                        io.write_all(b"N").await.map_err(|source| {
                            error(actual, Stage::Send, Reason::Io, DeliveryState::Unknown)
                                .with_source(source)
                        })?;
                        requested.send(()).unwrap();
                        finished.await.unwrap();
                        Ok(())
                    }),
                )?;
                assert!(send.send(Some(input)).is_ok());
                Err(ChannelError::retained(rejected))
            })
            .unwrap();
        let (peer, _) = fixture.listener.accept().await.unwrap();
        let input = setup.await.unwrap();
        let exit = scope.next_receipt().await.unwrap();
        assert!(auxiliary.matches_joined(exit.joined()));
        let received = original.accept_auxiliary().await.unwrap().unwrap();
        assert!(
            received.result().is_err() && received.failure_accepted(),
            "actual setup receipt: {:?}",
            received.result()
        );
        assert!(!received.registration_released());
        scope.acknowledge(&exit).unwrap();
        let acknowledged = auxiliary.wait_registration_released().await;
        assert!(auxiliary.matches_joined(&acknowledged));
        assert!(received.registration_released());
        let driver = if let Some(input) = input {
            let pending = original
                .take_preparation_job(&tasks, &received)
                .unwrap()
                .unwrap();
            pending
                .grant
                .accept_rejection(rejection, &received)
                .unwrap();
            let work = pending.work;
            Some(
                original
                    .spawn_preparation_job(&tasks, &pending.grant, input, |view| async move {
                        let _view = view;
                        work.await
                    })
                    .unwrap()
                    .1,
            )
        } else {
            assert!(original
                .take_preparation_job(&tasks, &received)
                .unwrap()
                .is_none());
            assert!(original.retire(deadline()).await.unwrap().is_complete());
            None
        };
        Self {
            original,
            fixture,
            scope,
            driver,
            peer,
            request,
            finish: Some(finish),
        }
    }
    async fn received(&mut self) {
        (&mut self.request).await.unwrap();
        let mut byte = [0; 1];
        self.peer.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, [b'N']);
    }
    async fn close(mut self, totals: &Arc<CarrierOriginalTotals>) {
        if let Some(driver) = self.driver {
            self.finish.take().unwrap().send(()).unwrap();
            let exit = self.scope.next_receipt().await.unwrap();
            assert!(
                exit.result().is_ok(),
                "actual job receipt: {:?}",
                exit.result()
            );
            assert!(driver.matches_joined(exit.joined()));
            assert!(!exit.joined().registration_released());
            self.scope.acknowledge(&exit).unwrap();
            let acknowledged = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&acknowledged) && acknowledged.registration_released());
            std::future::poll_fn(|cx| {
                totals.poll_cancelled_retirements(cx);
                if self.fixture.owner.retained() == 0 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
        assert!(self.original.closed().unwrap().matches(&self.original));
        assert!(self
            .original
            .users_closed()
            .unwrap()
            .matches(&self.original));
        assert_eq!(self.peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(self
            .fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        self.scope.begin_shutdown();
        assert!(self.scope.is_drained());
    }
}

#[tokio::test]
async fn original_root_jobs_and_requests_span_distinct_actual_owners_and_scopes() {
    timeout(Duration::from_secs(5), async {
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(3).unwrap());
        totals
            .install_preparation_limits_after_retirement(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        let mut first = JobFixture::new(&totals, ComponentId(901), false).await;
        first.received().await;
        let mut second = JobFixture::new(&totals, ComponentId(902), false).await;
        assert!(!first
            .original
            .retained_preparation_job()
            .unwrap()
            .retains(&second.original));
        assert!(!first
            .original
            .cleanup_owner()
            .unwrap()
            .same_owner(&second.original.cleanup_owner().unwrap(),));
        assert!(!first
            .driver
            .as_ref()
            .unwrap()
            .scope_identity()
            .same_scope(&second.driver.as_ref().unwrap().scope_identity(),));
        assert!(timeout(Duration::from_millis(20), &mut second.request)
            .await
            .is_err());
        let rejected = JobFixture::new(&totals, ComponentId(903), true).await;
        let usage = totals.preparation_snapshot();
        assert_eq!((usage.jobs, usage.requests), (2, 1));
        assert!(second.original.closed().is_none());
        rejected.close(&totals).await;
        first.close(&totals).await;
        second.received().await;
        let usage = totals.preparation_snapshot();
        assert_eq!((usage.jobs, usage.requests), (1, 1));
        second.close(&totals).await;
        assert_eq!(totals.snapshot().charged, 0);
        let usage = totals.preparation_snapshot();
        assert_eq!((usage.jobs, usage.requests), (0, 0));
        totals
            .install_preparation_limits_after_retirement(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
    })
    .await
    .unwrap();
}
