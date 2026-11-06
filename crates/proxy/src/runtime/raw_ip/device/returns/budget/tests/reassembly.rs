use std::{sync::Arc, time::Instant};
use zero_stack::{packet, OwnedFragmentOutcome, OwnedFragmentReassembler};
use zero_traits::PacketBuffer;
use zero_tun::packet_memory::{NativeReadGrant, PacketMemoryLimit, PacketReturnTotals};
const BYTES: usize = u16::MAX as usize + 40;
fn input(grant: &NativeReadGrant, bytes: &[u8]) -> PacketBuffer {
    let mut buffer = grant
        .try_buffer()
        .expect("the original IO read remains prepaid");
    buffer.as_mut_bytes()[..bytes.len()].copy_from_slice(bytes);
    match buffer.into_packet(bytes.len()) {
        Ok(packet) => packet,
        Err(_) => panic!("actual read fits admitted workspace"),
    }
}
#[tokio::test]
async fn charged_reassembly_keeps_authenticated_namespaces_and_full_root_read_progress() {
    for (source, target) in [("10.0.0.1", "10.0.0.2"), ("fd00::1", "fd00::2")] {
        let root = Arc::new(PacketReturnTotals::new(
            PacketMemoryLimit {
                frames: 1,
                bytes: 1,
            },
            PacketMemoryLimit {
                frames: 3,
                bytes: 3 * BYTES,
            },
        ));
        let grant = root.native_grant(BYTES).unwrap();
        let original = packet::build_udp(
            source.parse().unwrap(),
            target.parse().unwrap(),
            1234,
            4321,
            &[7; 2048],
        );
        let fragments = packet::fragment_ip_packet(&original, 576, 37);
        assert!(fragments.len() > 1);
        let mut cache = OwnedFragmentReassembler::<u64>::new();
        for source_session in [1, 2] {
            assert!(matches!(
                cache
                    .process(source_session, input(&grant, &fragments[0]), Instant::now())
                    .await,
                OwnedFragmentOutcome::Pending
            ));
        }
        assert_eq!(root.native().occupied.frames, 3);
        assert_eq!(root.native().occupied.bytes, 3 * BYTES);
        let mut completed = Vec::new();
        for source_session in [1, 2] {
            for fragment in &fragments[1..] {
                match cache
                    .process(source_session, input(&grant, fragment), Instant::now())
                    .await
                {
                    OwnedFragmentOutcome::Pending => {}
                    OwnedFragmentOutcome::Packet {
                        packet,
                        reassembled: true,
                    } => completed.push(packet),
                    _ => panic!("the same full root must finish its existing assembly"),
                }
            }
        }
        assert_eq!(completed.len(), 2);
        assert_eq!(cache.pending_datagrams(), 0);
        for output in &completed {
            assert_eq!(output.allocation_bytes(), BYTES);
            let decoded = packet::parse_udp(output).expect("reassembled real UDP packet");
            assert_eq!(decoded.payload, &[7; 2048]);
        }
        drop(cache);
        grant.revoke();
        assert_eq!(
            root.native().occupied.frames,
            2,
            "actual delivered assemblies still retain allocation credits"
        );
        drop(completed);
        assert_eq!(root.native().occupied.frames, 0);
        assert_eq!(root.native().occupied.bytes, 0);
    }
}
