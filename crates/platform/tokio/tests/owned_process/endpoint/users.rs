use super::*;
use std::{future::Future, task::Poll};
fn binding(running: &Running) -> UpstreamBinding {
    UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: running.handoff.spec.network_generation,
    }
}
fn context() -> CarrierContext {
    CarrierContext {
        session: None,
        deadline: Some(end()),
        cancel: CancellationToken::new(),
    }
}
#[tokio::test]
async fn process_endpoint_tcp_original_has_real_peer_and_cancel_retains_user_until_matching_close()
{
    let running = Running::start().await;
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    assert!(
        !scope
            .registrar()
            .scope_identity()
            .same_scope(&running.witness.scope_identity())
    );
    let mut owner = CarrierOwner::new(
        instance(720),
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    let fence = Arc::new(CancellationToken::new());
    let bound = owner
        .registration()
        .bind_declared(
            &ResourceDeclaration::ControlledOutboundStream {
                endpoint: endpoint(),
                binding: binding(&running),
                demand: ControlledStreamDemandClass::BackgroundControl,
                max_sockets: NonZeroUsize::new(1).unwrap(),
            },
            &scope.registrar(),
            &fence,
        )
        .unwrap();
    owner.start(end()).await.unwrap();
    let mut grant = owner.grants().reserve().unwrap();
    let view = grant.claim_transferred_view().unwrap();
    let forged = Arc::new(fence.as_ref().clone());
    assert!(
        running
            .handoff
            .authorize_consumer(&forged, bound.clone(), &grant)
            .is_err(),
        "same cancellation token value cannot reconstruct Host minting authority"
    );
    let use_ = running
        .handoff
        .authorize_consumer(&fence, bound, &grant)
        .unwrap();
    let native = NativeCarrierOpener::background_process_endpoint(
        &grant,
        binding(&running),
        endpoint(),
        use_,
        running.egress.clone(),
        None,
    )
    .unwrap();
    let transport = UpstreamTransport::new(binding(&running), Arc::new(native));
    let carrier = transport
        .prepare_controlled_stream(endpoint(), context(), grant)
        .unwrap()
        .open(end())
        .await
        .unwrap();
    assert_eq!(carrier.endpoints().peer, running.handoff.address().unwrap());
    let (io, actual) = carrier.into_parts();
    let mut loan = io.into_stream_loan(view).unwrap();
    assert!(
        loan.owner.original().parent().is_none(),
        "process dependency is not native encapsulation"
    );
    loan.io.write_all(b"echo").await.unwrap();
    let mut bytes = [0; 4];
    loan.io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"echo");
    let mut bytes = [0; 1];
    let mut reading = Box::pin(loan.io.read_exact(&mut bytes));
    std::future::poll_fn(|cx| {
        assert!(
            reading.as_mut().poll(cx).is_pending(),
            "actual native read must suspend"
        );
        Poll::Ready(())
    })
    .await;
    running.handoff.request_stop();
    assert!(reading.await.is_err());
    assert!(actual.closed().is_none());
    assert!(
        running
            .handoff
            .shutdown(Instant::now())
            .await
            .unwrap()
            .is_none(),
        "held unique view forbids false endpoint close"
    );
    drop(loan.io);
    assert!(loan.owner.close(end()).await.unwrap().is_complete());
    assert!(actual.closed().unwrap().matches(&actual));
    assert!(actual.users_closed().unwrap().matches(&actual));
    assert!(owner.shutdown(end()).await.unwrap().is_complete());
    assert!(scope.shutdown(end()).await.is_complete());
    running.finish().await;
}
#[tokio::test]
async fn process_endpoint_udp_uses_sameport_real_original_and_foreign_grant_is_rejected() {
    let running = Running::start().await;
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let mut owner = CarrierOwner::new(
        instance(721),
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    let fence = Arc::new(CancellationToken::new());
    let bound = owner
        .registration()
        .bind_declared(
            &ResourceDeclaration::ControlledDatagramCarrier {
                endpoint: endpoint(),
                binding: binding(&running),
                max_sockets: NonZeroUsize::new(1).unwrap(),
            },
            &scope.registrar(),
            &fence,
        )
        .unwrap();
    owner.start(end()).await.unwrap();
    let mut foreign = CarrierOwner::new(
        instance(722),
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    assert!(!foreign.registration().same_owner(&owner.registration()));
    foreign.start(end()).await.unwrap();
    let foreign_grant = foreign.grants().reserve().unwrap();
    assert!(
        running
            .handoff
            .authorize_consumer(&fence, bound.clone(), &foreign_grant)
            .is_err()
    );
    drop(foreign_grant);
    let mut grant = owner.grants().reserve().unwrap();
    let actual = grant.adoption();
    let view = grant.claim_transferred_view().unwrap();
    let permit = running
        .handoff
        .authorize_consumer(&fence, bound.clone(), &grant)
        .unwrap();
    let native = NativeCarrierOpener::background_process_endpoint(
        &grant,
        binding(&running),
        endpoint(),
        permit,
        running.egress.clone(),
        None,
    )
    .unwrap();
    use crate::contract::network::UpstreamOpener;
    let packet = native
        .prepare_controlled_datagram(&binding(&running), endpoint(), context(), grant)
        .unwrap();
    packet.send(b"udp").await.unwrap();
    let mut bytes = [0; 3];
    assert_eq!(packet.receive(&mut bytes).await.unwrap(), 3);
    assert_eq!(&bytes, b"udp");
    drop(packet);
    drop(view);
    assert!(actual.retire(end()).await.unwrap().is_complete());
    assert!(actual.closed().unwrap().matches(&actual));
    assert!(actual.users_closed().unwrap().matches(&actual));
    assert!(owner.shutdown(end()).await.unwrap().is_complete());
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(foreign.shutdown(end()).await.unwrap().is_complete());
    running.finish().await;
}
