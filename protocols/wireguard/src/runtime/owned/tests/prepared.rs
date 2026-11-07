//! Real established keys; prepared receive has no replay/stats acceptance.
use super::*;
fn data(work: &NativeReadGrant, wire: &[u8]) -> gotatun::packet::Packet<gotatun::packet::WgData> {
    let packet = Packet::from_owned_buffer(received(work, wire))
        .unwrap_or_else(|_| panic!("same Root actual native allocation"));
    match packet.try_into_wg().unwrap() {
        gotatun::packet::WgKind::Data(data) => data,
        _ => panic!("actual peer returned data"),
    }
}
#[test]
fn actual_prepared_data_drop_wrong_session_and_replay_leave_commit_boundary_unchanged() {
    let mut client = tunnel(1, 2, "10.0.0.1/32");
    let mut server = tunnel(2, 1, "10.0.0.2/32");
    let initiation = networks(client.initiate_handshake().unwrap());
    let response = networks(server.receive_datagram(source(), &initiation[0]).unwrap());
    let keepalive = networks(client.receive_datagram(source(), &response[0]).unwrap());
    server.receive_datagram(source(), &keepalive[0]).unwrap();
    let mut payload = ip();
    payload[12..16].copy_from_slice(&[10, 0, 0, 2]);
    payload[16..20].copy_from_slice(&[10, 0, 0, 1]);
    let wire = networks(server.send_ip_packet(&payload).unwrap());
    let roots = root(4, 1024);
    let work = roots.datagram_workspaces([256; 2], None).unwrap();
    let full = roots.reserve_transmit(&[1; 4]).unwrap();
    assert_eq!(roots.transmit().occupied.frames, 4);
    let before = client.engine.stats();
    let (prepared, plain) = client
        .engine
        .prepare_owned_data(data(&work.reads[0], &wire[0]))
        .unwrap();
    assert_eq!(
        (client.engine.stats().1, client.engine.stats().2),
        (before.1, before.2)
    );
    assert_eq!(&plain[..payload.len()], &payload);
    drop(prepared);
    drop(plain); // actual memory return, no receive acceptance
    let (wrong, plain) = client
        .engine
        .prepare_owned_data(data(&work.reads[0], &wire[0]))
        .unwrap();
    assert!(
        server.engine.commit_owned_data(wrong).is_err(),
        "another real Tunn cannot commit this actual Session"
    );
    drop(plain);
    let (first, plain) = client
        .engine
        .prepare_owned_data(data(&work.reads[0], &wire[0]))
        .unwrap();
    let (duplicate, second) = client
        .engine
        .prepare_owned_data(data(&work.reads[1], &wire[0]))
        .unwrap();
    assert_eq!(
        (client.engine.stats().1, client.engine.stats().2),
        (before.1, before.2)
    );
    client.engine.commit_owned_data(first).unwrap();
    let committed = client.engine.stats();
    assert_eq!(committed.2, before.2 + plain.len());
    assert!(client.engine.commit_owned_data(duplicate).is_err());
    assert_eq!(
        (client.engine.stats().1, client.engine.stats().2),
        (committed.1, committed.2)
    );
    assert_eq!(
        roots.transmit().occupied.frames,
        4,
        "prepared read never took an uncharged TX buffer"
    );
    drop(plain);
    drop(second);
    drop(full);
    assert_eq!(roots.transmit().occupied.frames, 0);
    assert!(work.reads.iter().all(NativeReadGrant::workspace_returned));
}

#[test]
fn actual_owned_protocol_preparation_can_cancel_and_retry_before_replay_at_full_transmit() {
    let mut client = tunnel(1, 2, "10.0.0.1/32");
    let mut server = tunnel(2, 1, "10.0.0.2/32");
    let initiation = networks(client.initiate_handshake().unwrap());
    let response = networks(server.receive_datagram(source(), &initiation[0]).unwrap());
    let keepalive = networks(client.receive_datagram(source(), &response[0]).unwrap());
    server.receive_datagram(source(), &keepalive[0]).unwrap();
    let mut payload = ip();
    payload[12..16].copy_from_slice(&[10, 0, 0, 2]);
    payload[16..20].copy_from_slice(&[10, 0, 0, 1]);
    let wire = networks(server.send_ip_packet(&payload).unwrap());
    let roots = root(4, 1024);
    let work = roots.datagram_workspaces([256; 2], Some(148)).unwrap();
    let full = roots.reserve_transmit(&[1; 3]).unwrap();
    let control = || {
        work.control
            .as_ref()
            .unwrap()
            .try_buffer()
            .unwrap()
            .into_packet(148)
            .unwrap_or_else(|_| panic!("same prepaid actual control"))
    };
    let prepare = |client: &mut PeerTunnel| {
        client.prepare_datagram_owned(
            source(),
            received(&work.reads[0], &wire[0]),
            work.reads[1].try_buffer().unwrap(),
            control(),
        )
    };
    let before = client.engine.stats();
    let first = prepare(&mut client);
    drop(first.unused_control);
    let crate::runtime::PreparedReceive::Data(first) = first.result.unwrap() else {
        panic!("actual Data");
    };
    assert!(client.has_pending_packets());
    assert!(
        work.reads[0].workspace_returned(),
        "original ciphertext was consumed by the completed AEAD copy"
    );
    let (token, packet, _) = first.into_parts();
    assert_eq!(&*packet, &payload);
    assert_eq!(roots.transmit().occupied.frames, 4);
    drop(token);
    drop(packet); // no Stack delivery/replay acceptance
    assert!(!client.has_pending_packets());
    assert_eq!(client.engine.stats().2, before.2);
    let next = prepare(&mut client);
    drop(next.unused_control);
    let crate::runtime::PreparedReceive::Data(next) = next.result.unwrap() else {
        panic!("same uncommitted Data retries");
    };
    let (token, packet, _) = next.into_parts();
    assert!(client.commit_prepared_data(&Arc::new(())).is_err());
    assert!(
        client.has_pending_packets(),
        "foreign identity cannot consume the actual token"
    );
    client.commit_prepared_data(&token).unwrap();
    drop(packet);
    drop(token);
    let replay = prepare(&mut client);
    assert!(replay.result.is_err());
    drop(replay);
    assert_eq!(roots.transmit().occupied.frames, 4);
    assert!(work.reads.iter().all(NativeReadGrant::workspace_returned));
    drop(full);
}
