use super::*;
use crate::contract::component::{ControlledStreamDemandClass, ResourceDeclaration};
use std::task::Poll;
fn description(fixture: &Fixture, n: usize) -> ResourceDeclaration {
    ResourceDeclaration::ControlledOutboundStream {
        endpoint: fixture.destination.clone(),
        binding: fixture.transport.binding().clone(),
        demand: ControlledStreamDemandClass::BackgroundControl,
        max_sockets: NonZeroUsize::new(n).unwrap(),
    }
}
async fn tracked(fixture: &Fixture) -> (ControlledCarrier, TcpStream, CarrierUserView) {
    let mut grant = fixture.grants.reserve().unwrap();
    let view = grant.claim_transferred_view().unwrap();
    let carrier = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap()
        .open(deadline())
        .await
        .unwrap();
    let (peer, source) = fixture.listener.accept().await.unwrap();
    assert_eq!(
        source.ip(),
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    (carrier, peer, view)
}
#[tokio::test]
async fn same_host_two_real_owners_share_total_before_io_and_cannot_exchange_their_local_limits() {
    timeout(Duration::from_secs(3), async {
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut first = Fixture::cold(2).await;
        let mut second = Fixture::cold(2).await;
        let host = Arc::new(CancellationToken::new());
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        for fixture in [&mut first, &mut second] {
            fixture
                .owner
                .registration()
                .bind_declared_with_totals(
                    &description(fixture, 2),
                    &scope.registrar(),
                    &host,
                    &totals,
                )
                .unwrap();
            fixture.owner.start(deadline()).await.unwrap();
        }
        let foreign = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        // A live actual owner cannot swap roots or shed already charged IO.
        assert!(
            first
                .owner
                .registration()
                .bind_declared_with_totals(
                    &description(&first, 2),
                    &scope.registrar(),
                    &host,
                    &foreign,
                )
                .is_err()
        );
        let (carrier, mut peer, view) = tracked(&first).await;
        let (mut io, actual) = carrier.into_parts();
        io.write_all(b"original").await.unwrap();
        let mut bytes = [0; 8];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"original");
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(second.owner.retained(), 0);
        let error = second.grants.reserve().err().unwrap();
        assert_eq!(error.reason, Reason::Capacity);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert_eq!(second.owner.retained(), 0);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(second.listener.poll_accept(cx).is_pending()))
                .await
        );
        assert!(totals
            .install_after_retirement(NonZeroUsize::new(2).unwrap())
            .is_err());
        assert!(actual.closed().is_none());
        drop(io);
        drop(view);
        assert!(actual.retire(deadline()).await.unwrap().is_complete());
        let native = actual.closed().unwrap();
        assert!(native.matches(&actual) && native.had_native_acquisition());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(totals.snapshot().charged, 0);
        let (next, mut next_peer, next_view) = tracked(&second).await;
        let (io, actual) = next.into_parts();
        drop(io);
        drop(next_view);
        assert!(actual.retire(deadline()).await.unwrap().is_complete());
        assert!(actual.closed().unwrap().had_native_acquisition());
        assert_eq!(next_peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(totals.snapshot().charged, 0);
        for fixture in [&mut first, &mut second] {
            assert!(fixture
                .owner
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
        }
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn owner_loss_cannot_drop_original_native_or_refund_total_and_existing_inline_host_retires_it(
) {
    timeout(Duration::from_secs(3), async {
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut fixture = Fixture::cold(1).await;
        let host = Arc::new(CancellationToken::new());
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let registration = fixture.owner.registration();
        registration
            .bind_declared_with_totals(
                &description(&fixture, 1),
                &scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (carrier, mut peer, view) = tracked(&fixture).await;
        let (io, actual) = carrier.into_parts();
        let original = registration.observe().unwrap().slots()[0]
            .original()
            .clone();
        drop(fixture); // Actual Owner + all spare grant clones are gone.
        assert_eq!(totals.snapshot().charged, 1);
        assert!(registration.observe().unwrap().is_stopped());
        assert!(original.closed().is_none());
        assert!(registration.closed().is_none());
        drop(io);
        drop(view);
        drop(actual);
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let native = original
            .closed()
            .expect("actual retained controller was explicitly closed");
        assert!(original.matches_closed(&native));
        assert!(native.had_native_acquisition());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        // A disappeared owner is never promoted to a generation-close receipt.
        assert!(registration.closed().is_none());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn root_keeps_real_driver_original_after_native_users_close_until_same_host_ack() {
    timeout(Duration::from_secs(3), async {
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut fixture = Fixture::cold(2).await;
        let host = Arc::new(CancellationToken::new());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        fixture
            .owner
            .registration()
            .bind_declared_with_totals(
                &description(&fixture, 2),
                &scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let driver_view = grant.claim_driver_view().unwrap();
        let carrier = fixture
            .transport
            .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (io, actual) = carrier.into_parts();
        let (_, witness) = actual
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Auxiliary,
                io.into_driver(),
                move |mut io| async move {
                    let _driver_view = driver_view;
                    io.write_all(b"same driver").await.map_err(|source| {
                        error(instance(), Stage::Send, Reason::Io, DeliveryState::Unknown)
                            .with_source(source)
                    })?;
                    Ok(())
                },
            )
            .unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(receipt.result().is_ok() && witness.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert!(actual.retire(deadline()).await.unwrap().is_complete());
        assert!(actual.closed().unwrap().matches(&actual));
        assert!(actual.closed().unwrap().had_native_acquisition());
        assert!(actual.users_closed().unwrap().matches(&actual));
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"same driver");
        assert_eq!(fixture.owner.retained(), 0);
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        scope.acknowledge(&receipt).unwrap();
        let joined = witness.wait_registration_released().await;
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        assert_eq!(totals.snapshot().charged, 0);
        drop(fixture.grants.reserve().unwrap());
        assert_eq!(totals.snapshot().charged, 0);
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

mod metadata;

mod bound;
