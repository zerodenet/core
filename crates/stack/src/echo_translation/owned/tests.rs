//! A cached Echo cannot wait for its own Root debit to restore the response.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
#[tokio::test]
async fn full_transmit_root_restores_prepaid_response_and_retains_originals_during_real_pending_write(
) {
    let limit = PacketMemoryLimit {
        frames: 32,
        bytes: 32_768,
    };
    let root = Arc::new(PacketReturnTotals::new_with_transmit(limit, limit, limit));
    let bytes = request();
    let mut original = root.transmit_packet(bytes.len(), 0, 0).unwrap();
    original.copy_from_slice(&bytes);
    let pointer = original.as_ptr();
    let mut table = EchoTranslation::default();
    let mut outgoing = table
        .request_batch(
            original,
            "10.0.0.3".parse().unwrap(),
            7,
            1500,
            PacketWireMemory::Native,
            1500,
            PacketWireMemory::Native,
            Instant::now(),
        )
        .unwrap();
    assert_eq!(
        table.pending.values().next().unwrap().original.as_ptr(),
        pointer
    );
    let reply = packet::build_local_icmp_echo_reply(&outgoing.remove(0), 1500).unwrap();
    drop(outgoing);
    let occupied = root.transmit().occupied;
    // Install the exact already-paid limit. No extra transmit capacity remains
    // for a response allocator, even though its real request cache is live.
    root.install_transmit(occupied.frames, occupied.bytes)
        .unwrap();
    let reads = root.native_grant(reply.len()).unwrap();
    let mut received = reads.try_buffer().unwrap();
    received.as_mut_bytes().copy_from_slice(&reply);
    let received = received
        .into_packet(reply.len())
        .unwrap_or_else(|_| panic!("same native length"));
    let restored = match table
        .response_batch(received, 1500, PacketWireMemory::Native, Instant::now())
        .unwrap()
    {
        OwnedEchoResponse::Response(context, restored) => {
            assert_eq!(context, 7);
            restored
        }
        OwnedEchoResponse::Unmatched(_) => panic!("actual translated reply must match"),
    };
    assert!(table.pending.is_empty());
    assert_eq!(root.transmit().occupied, occupied);
    assert!(
        !reads.workspace_returned(),
        "same native original is retained by actual restored output"
    );
    let (mut writer, mut reader) = tokio::io::duplex(1);
    {
        let write = writer.write_all(&restored);
        tokio::pin!(write);
        assert!(
            std::future::poll_fn(|cx| {
                use std::future::Future;
                std::task::Poll::Ready(write.as_mut().poll(cx).is_pending())
            })
            .await
        );
        assert_eq!(root.transmit().occupied, occupied);
        let mut wire = vec![0; restored.len()];
        let (sent, read) = tokio::join!(&mut write, reader.read_exact(&mut wire));
        sent.unwrap();
        read.unwrap();
        assert_eq!(&wire[..], &restored[..]);
    }
    drop(restored);
    assert!(reads.workspace_returned());
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    reads.revoke();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
fn request() -> Vec<u8> {
    let source = "10.0.0.1".parse().unwrap();
    let destination = "10.0.0.2".parse().unwrap();
    let mut message = vec![8, 0, 0, 0, 0, 7, 0, 1];
    message.extend_from_slice(b"actual-owned-response");
    let sum = packet::checksum(&message);
    message[2..4].copy_from_slice(&sum.to_be_bytes());
    packet::build_icmp_echo_tunnel_probe(
        &packet::IcmpEchoRequest {
            source,
            destination,
            message: &message,
        },
        7,
        source,
        1500,
    )
    .unwrap()
}
