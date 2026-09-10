#![cfg(feature = "crypto")]
use mieru::udp::{MieruInboundUdpResponder, MieruInboundUdpSession};
use tokio::io::AsyncWriteExt;
use zero_core::Address;

#[tokio::test]
async fn fragmented_udp_frames_survive_cancelled_reads_and_coalescing() {
    let target = Address::Ipv4([127, 0, 0, 1]);
    let payload = vec![42; 1600];
    let mut wire = Vec::new();
    MieruInboundUdpSession::new()
        .write_response_for_target_tokio(&mut wire, &target, 53, &payload)
        .await
        .unwrap();
    let (mut sender, mut receiver) = tokio::io::duplex(8192);
    let mut responder = MieruInboundUdpResponder::default();
    for part in [&wire[..2], &wire[2..1100]] {
        sender.write_all(part).await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            responder.read_inbound_dispatch_tokio(&mut receiver)
        )
        .await
        .is_err());
    }
    sender.write_all(&wire[1100..]).await.unwrap();
    sender.write_all(&wire).await.unwrap();
    sender.shutdown().await.unwrap();
    for _ in 0..2 {
        let dispatch = responder
            .read_inbound_dispatch_tokio(&mut receiver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dispatch.payload(), payload);
    }
    assert!(responder
        .read_inbound_dispatch_tokio(&mut receiver)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn session_reader_preserves_partial_frame_when_moved_into_responder() {
    let session = MieruInboundUdpSession::new();
    let target = Address::Ipv4([127, 0, 0, 1]);
    let payload = vec![73; 1600];
    let mut wire = Vec::new();
    session
        .write_response_for_target_tokio(&mut wire, &target, 5353, &payload)
        .await
        .unwrap();
    let (mut sender, mut receiver) = tokio::io::duplex(8192);
    // The legacy scratch buffer no longer sets a packet-size limit.
    let mut scratch = [0; 1];
    for part in [&wire[..2], &wire[2..1100]] {
        sender.write_all(part).await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            session.read_dispatch_parts_tokio(&mut receiver, &mut scratch)
        )
        .await
        .is_err());
    }
    sender.write_all(&wire[1100..]).await.unwrap();
    sender.write_all(&wire).await.unwrap();
    let first = session
        .read_inbound_dispatch_tokio(&mut receiver, &mut scratch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), payload);
    // Moving ownership must retain framing state across cancelled reads too.
    let second = session
        .read_dispatch_parts_tokio(&mut receiver, &mut scratch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second.into_parts(),
        (target.clone(), 5353, payload.clone(), None)
    );
    sender.write_all(&wire[..2]).await.unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(10),
        session.read_dispatch_parts_tokio(&mut receiver, &mut scratch)
    )
    .await
    .is_err());
    let mut responder = MieruInboundUdpResponder::new(session);
    sender.write_all(&wire[2..]).await.unwrap();
    sender.shutdown().await.unwrap();
    assert_eq!(
        responder
            .read_inbound_dispatch_tokio(&mut receiver)
            .await
            .unwrap()
            .unwrap()
            .payload(),
        payload
    );
    assert!(responder
        .read_inbound_dispatch_tokio(&mut receiver)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn session_reader_rejects_eof_inside_a_frame() {
    for wire in [vec![0, 0], vec![0, 0, 7, 1, 127]] {
        let session = MieruInboundUdpSession::new();
        let mut reader = wire.as_slice();
        let error = session
            .read_dispatch_parts_tokio(&mut reader, &mut [])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            zero_core::Error::Protocol("mieru udp: truncated")
        ));
    }
}
