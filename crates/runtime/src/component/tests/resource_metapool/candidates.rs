use super::*;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::TaskClass,
    },
    controlled_carrier::{CarrierCandidates, NativeCarrierOpener},
};

fn destination(peer: SocketAddr) -> Destination {
    Destination::new(
        match peer.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
async fn candidates(
    fixture: &Fixture,
    peer: SocketAddr,
    egress: Arc<zero_platform_tokio::EgressInterfaceControl>,
    cancel: zero_platform_tokio::contract::CancellationToken,
) -> CarrierCandidates {
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    NativeCarrierOpener::new(
        fixture.slot.instance_id(),
        binding.clone(),
        peer,
        egress,
        None,
    )
    .unwrap()
    .resolve_controlled_carrier(
        &binding,
        destination(peer),
        CarrierContext {
            session: None,
            deadline: Some(end()),
            cancel,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn selected_metadata_original_requires_pending_io_join_view_return_and_native_receipt_separately(
) {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = peer.local_addr().unwrap();
    let fixture = Fixture::dynamic(destination(peer), vec![], 1).await;
    let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
    let selected = candidates(&fixture, peer, egress, Default::default()).await;
    let (query, release) = fixture.query();
    let permit = fixture.grants.reserve(&query).unwrap();
    let original = permit.origin();
    let (_, mut grant) = selected
        .open_metadata(0, None, &permit, &query, end())
        .unwrap();
    let lineage =
        zero_platform_tokio::controlled_carrier::CarrierOrigin::from_metadata(original.clone());
    let registered = fixture.slot.original_metadata_owner(0).unwrap();
    assert!(lineage.owner().same_owner(
        zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Metadata(&registered)
    ));
    assert!(lineage.endpoints().unwrap().matches_metadata(&original));
    assert_eq!(lineage.selected_binding().unwrap().0, destination(peer));
    assert!(!lineage.is_retired());
    assert!(lineage.closed().is_none());
    let retained_tx = grant.tx.clone();
    let claim = grant.users_claim.take().unwrap();
    let (graph, mut receiver) = claim
        .bind_users::<(), ()>(fixture.tasks.clone(), n(1), n(1))
        .unwrap_or_else(|_| panic!("same original scope"));
    let completion = graph.completion();
    let user = graph.users().acquire().unwrap();
    let stop = graph.users().cancellation().clone();
    let worker_stop = stop.clone();
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let driver = graph.drivers().spawn((), TaskClass::Auxiliary, move |body| async move {
        let mut bytes = [0u8; 64];
        let mut buffers = [std::io::IoSliceMut::new(&mut bytes)];
        let mut metadata = [zero_platform_tokio::controlled_datagram::metadata::MetadataReceive::default()];
        let mut pending_tx = Some(pending_tx);
        let receive = std::future::poll_fn(|cx| {
            let result = grant.rx.poll_recv(cx, &mut buffers, &mut metadata);
            if result.is_pending() {
                if let Some(tx) = pending_tx.take() { let _ = tx.send(()); }
            }
            result
        });
        tokio::pin!(receive);
        tokio::select! {
            biased;
            _ = worker_stop.cancelled() => {},
            result = &mut receive => panic!("empty original unexpectedly completed: {result:?}"),
        }
        drop(grant);
        drop(user);
        body.record(());
        Ok(())
    }).unwrap();
    fixture.drive(pending_rx).await.unwrap();
    assert!(completion.try_closed().is_none());
    assert!(permit.close(None, &fixture.tasks).is_err());
    assert!(fixture.grants.reserve(&query).is_err());
    stop.cancel();
    graph.drivers().revoke_registration();
    let joined = fixture
        .drive(graph.drivers().wait_joined(end()))
        .await
        .unwrap();
    assert_eq!(joined.len(), 1);
    assert!(joined[0].result().is_ok());
    assert!(
        completion.try_closed().is_none(),
        "actual exit must also be received"
    );
    let delivered = fixture
        .drive(receiver.receive(driver, end()))
        .await
        .unwrap();
    assert!(delivered.joined.result().is_ok());
    assert!(
        completion.try_closed().is_none(),
        "transferred TX view is still live"
    );
    assert!(
        original.closed().is_none(),
        "driver join does not close the native original"
    );
    drop(retained_tx);
    let users = completion
        .try_closed()
        .expect("actual joins, reception and views returned");
    assert!(original.closed().is_none());
    let closed = permit.close(Some(&users), &fixture.tasks).unwrap();
    assert!(closed.matches(&original) && closed.had_native_acquisition());
    assert!(lineage.is_retired());
    let native_closed = lineage.closed().unwrap();
    assert!(native_closed.matches(&lineage) && native_closed.had_native_acquisition());
    let next = fixture.grants.reserve(&query).unwrap();
    assert!(!closed.matches(&next.origin()));
    assert!(!native_closed.matches(
        &zero_platform_tokio::controlled_carrier::CarrierOrigin::from_metadata(next.origin())
    ));
    assert!(!next
        .close(None, &fixture.tasks)
        .unwrap()
        .had_native_acquisition());
    release.send(()).unwrap();
    fixture.drive(query.wait()).await;
    assert!(fixture.finish().await.is_complete());
}

#[tokio::test]
async fn metadata_candidates_preserve_original_owner_port_and_network_authority_before_acquisition()
{
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let hop = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let peer = peer.local_addr().unwrap();
    let hop = hop.local_addr().unwrap();
    let fixture = Fixture::dynamic(destination(peer), vec![hop.port()], 1).await;
    let foreign = Fixture::dynamic(destination(peer), vec![hop.port()], 1).await;
    let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
    let selected = candidates(&fixture, peer, egress.clone(), Default::default()).await;
    let (query, release) = fixture.query();
    let (other, other_release) = foreign.query();
    let permit = fixture.grants.reserve(&query).unwrap();
    let other_permit = foreign.grants.reserve(&other).unwrap();
    assert!(selected
        .open_metadata(0, Some(hop.port()), &other_permit, &other, end())
        .is_err());
    assert!(selected
        .open_metadata(0, None, &permit, &query, end())
        .is_err());
    assert!(permit.origin().endpoints().is_none());
    let (actual_peer, grant) = selected
        .open_metadata(0, Some(hop.port()), &permit, &query, end())
        .unwrap();
    assert_eq!(actual_peer, hop);
    let returned = grant
        .return_unstarted()
        .unwrap_or_else(|_| panic!("matching unique original"));
    let users = returned.try_closed().unwrap();
    let closed = permit.close(Some(&users), &fixture.tasks).unwrap();
    assert!(closed.had_native_acquisition());
    let next = fixture.grants.reserve(&query).unwrap();
    egress.invalidate_network();
    assert!(selected
        .open_metadata(0, Some(hop.port()), &next, &query, end())
        .is_err());
    assert!(next.origin().endpoints().is_none());
    assert!(!next
        .close(None, &fixture.tasks)
        .unwrap()
        .had_native_acquisition());
    assert!(!other_permit
        .close(None, &foreign.tasks)
        .unwrap()
        .had_native_acquisition());
    release.send(()).unwrap();
    other_release.send(()).unwrap();
    fixture.drive(query.wait()).await;
    foreign.drive(other.wait()).await;
    assert!(fixture.finish().await.is_complete());
    assert!(foreign.finish().await.is_complete());
}
