use super::*;

#[tokio::test]
async fn later_protocol_receipt_preserves_the_original_wire_map_and_real_pending_host_ack() {
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(2)));
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let mut native = MetadataOwner::new(limits()).unwrap();
    let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let (issuer, claim) = grant
        .control
        .claim_views(registrar.scope_identity())
        .unwrap();
    let ((wire, mut wire_rx), (protocol, mut protocol_rx)) = claim
        .bind_packet_endpoint::<u8, ()>(registrar.clone(), bound(1), bound(1))
        .ok()
        .unwrap();
    let mut peers = Vec::new();
    let mut ids = Vec::new();
    for graph in [&wire, &protocol] {
        let (mut io, peer) = tokio::io::duplex(1);
        let (pending_tx, pending_rx) = oneshot::channel();
        let id = graph
            .drivers()
            .spawn(
                1,
                crate::contract::scope::TaskClass::Auxiliary,
                |body| async move {
                    let mut pending = Some(pending_tx);
                    let read = io.read_u8();
                    tokio::pin!(read);
                    let result = poll_fn(|cx| {
                        let result = read.as_mut().poll(cx);
                        if result.is_pending() {
                            if let Some(pending) = pending.take() {
                                let _ = pending.send(());
                            }
                        }
                        result
                    })
                    .await;
                    assert!(
                        result.is_err(),
                        "the original duplex read receives actual EOF"
                    );
                    body.record(());
                    Ok(())
                },
            )
            .unwrap();
        pending_rx.await.unwrap();
        graph.drivers().revoke_registration();
        graph.users().cancellation().cancel();
        peers.push(peer);
        ids.push(id);
    }
    drop(grant.rx);
    drop(grant.tx);
    drop(peers.remove(0));
    let wire_ack = scope.next_receipt().await.unwrap();
    assert_eq!(wire_ack.id(), ids[0]);
    let wire_exit = wire_rx.receive_original(ids[0]).await.unwrap();
    wire_rx.release_body(&wire_exit).unwrap();
    let first = wire.completion().try_owned_closed().unwrap();
    assert!(issuer.validates_users(&first, &registrar));
    assert!(
        protocol.drivers().owned_registration_completion().is_none(),
        "actual protocol read is still pending"
    );
    grant.control.close().unwrap();
    assert_eq!(native.take_report().main.confirmed_closed, 1);
    scope.acknowledge(&wire_ack).unwrap();
    drop(peers.remove(0));
    let protocol_ack = scope.next_receipt().await.unwrap();
    assert_eq!(protocol_ack.id(), ids[1]);
    let protocol_exit = protocol_rx.receive_original(ids[1]).await.unwrap();
    protocol_rx.release_body(&protocol_exit).unwrap();
    let final_map = protocol.drivers().owned_registration_completion().unwrap();
    let later = first
        .clone()
        .with_related_driver_receipt(final_map)
        .ok()
        .unwrap();
    let completed = first.with_later_protocol_receipt(&later).unwrap();
    assert!(completed.same_original(&first));
    assert_eq!(
        completed.registrations().next().unwrap().task_id().unwrap(),
        ids[0]
    );
    let original_protocol = completed.related_registrations().next().unwrap();
    assert!(original_protocol.matches_joined(&protocol_exit.joined));
    assert!(
        !protocol_exit.joined.registration_released(),
        "protocol body/native closure never substitutes Host ACK"
    );
    scope.acknowledge(&protocol_ack).unwrap();
    let released = original_protocol.wait_registration_released().await;
    assert!(released.registration_released());
    assert!(
        completed.with_later_protocol_receipt(&first).is_none(),
        "a later observation cannot erase the actual protocol map"
    );
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
