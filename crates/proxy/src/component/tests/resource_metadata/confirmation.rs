//! Same actual Slot/Scope/native receiver, independently of Quinn's ADTs.
use super::{canonical::CanonicalMetadata, *};
use zero_platform_tokio::controlled_datagram::metadata::MetadataReceive;

#[tokio::test]
async fn canonical_private_confirmation_requires_actual_received_join_and_last_logical_guard() {
    let (fixture, mut grant, tasks) = CanonicalMetadata::start(loopback(), limits(), n(1)).await;
    let claim = grant.users_claim.take().unwrap();
    let (graph, mut receiver) = claim
        .bind_users::<u8, ()>(tasks.clone(), n(1), n(1))
        .ok()
        .unwrap();
    let completion = graph.completion();
    let logical = graph.users().acquire().unwrap();
    let local = grant.tx.local_addr().unwrap();
    let peer = tokio::net::UdpSocket::bind(loopback()).await.unwrap();
    let remote = peer.local_addr().unwrap();
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let tx = grant.tx;
    let mut rx = grant.rx;
    let id = graph
        .drivers()
        .spawn(1, TaskClass::Auxiliary, |body| async move {
            let mut bytes = [0; 1];
            let mut buffers = [IoSliceMut::new(&mut bytes)];
            let mut metadata = [MetadataReceive::default()];
            let mut pending_tx = Some(pending_tx);
            let count = poll_fn(|cx| {
                let result = rx.poll_recv(cx, &mut buffers, &mut metadata);
                if result.is_pending() {
                    if let Some(notice) = pending_tx.take() {
                        notice.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .unwrap();
            assert_eq!(count, 1);
            assert_eq!(metadata[0].source, remote);
            assert_eq!(metadata[0].len, 0);
            body.record(());
            drop(tx);
            drop(rx);
            Ok(())
        })
        .unwrap();
    fixture.drive(pending_rx).await.unwrap(); // Actual original native RX Pending.
    assert!(completion.try_closed().is_none());
    peer.send_to(b"", local).await.unwrap();
    graph.drivers().revoke_registration();
    graph.users().cancellation().cancel();
    fixture
        .drive(graph.drivers().wait_joined(deadline()))
        .await
        .unwrap();
    assert!(
        completion.try_closed().is_none(),
        "join did not receive outcome"
    );
    receiver.receive(id, deadline()).await.unwrap();
    assert_eq!(graph.users().active(), 1);
    assert!(
        completion.try_closed().is_none(),
        "all native views and actual drivers ended, original logical lease remains"
    );
    drop(logical);
    let receipt = completion.try_closed().unwrap();
    fixture.confirm_users(receipt, &tasks).unwrap();
    let before = fixture.resources().io_evidence();
    assert_eq!(before.metadata[0].confirmed_users(), 1);
    assert!(
        !before.physical_confirmed(),
        "the native original remains open"
    );
    let report = fixture.shutdown().await;
    assert!(report.is_complete());
    let after = fixture.resources().io_evidence();
    assert_eq!(after.metadata[0].main.confirmed_closed, 1);
    assert!(after.physical_confirmed());
}
