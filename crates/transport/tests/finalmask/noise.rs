use super::*;
use crate::finalmask::udp::{Codec, Mask};
fn settings() -> Settings {
    Settings {
        reset_seconds: Range {
            minimum: 2,
            maximum: 2,
        },
        items: vec![Item {
            packet: b"noise".to_vec(),
            random_length: Range::default(),
            minimum_byte: 0,
            maximum_byte: 255,
            delay_ms: Range {
                minimum: 3,
                maximum: 3,
            },
        }],
    }
}
#[tokio::test]
async fn paid_noise_history_keeps_existing_peer_live_while_foreign_root_pressure_refuses_only_new_rows(
) {
    use std::{num::NonZeroUsize, sync::Arc};
    use zero_platform_tokio::controlled_carrier::CarrierOriginalTotals;
    use zero_tun::packet_memory::{PacketMetadataLayout, PacketReturnTotals};
    let original = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let book = original.buffer_budget();
    let root = Arc::new(PacketReturnTotals::default());
    let metadata = root.driver_metadata_view();
    let baseline = metadata.snapshot().occupied;
    let memory =
        crate::finalmask::memory::Memory::prepare(book.clone(), metadata.clone(), 65536, 65536)
            .unwrap();
    let mut config = settings();
    config.reset_seconds = Range::default();
    let mut codec = Codec::new(&[Mask::Noise(config)], false).unwrap();
    codec.install_memory(memory.clone()).unwrap();
    let first = "127.0.0.1:8001".parse().unwrap();
    let second = "127.0.0.1:8002".parse().unwrap();
    codec.prepare_peer(first).unwrap();
    assert_eq!(codec.encode_for(first, b"first").unwrap().len(), 2);
    codec.finish_noise(first);
    let snapshot = metadata.snapshot();
    let foreign = metadata
        .reserve(
            PacketMetadataLayout::array::<u8>(snapshot.limit.bytes() - snapshot.occupied.bytes())
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        codec.prepare_peer(second).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(codec.encode_for(first, b"still live").unwrap().len(), 1);
    drop(foreign);
    codec.prepare_peer(second).unwrap();
    assert_eq!(codec.encode_for(second, b"new").unwrap().len(), 2);
    assert_eq!(codec.encode_for(first, b"same history").unwrap().len(), 1);
    drop(codec);
    drop(memory);
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot().occupied, baseline);
}
#[tokio::test(start_paused = true)]
async fn noise_reset_begins_after_send_and_is_scoped_to_peer_and_chain_stage() {
    let peer = "127.0.0.1:9000".parse().unwrap();
    let other = "127.0.0.1:9001".parse().unwrap();
    let mut codec = Codec::new(
        &[Mask::Wireguard, Mask::Noise(settings()), Mask::MkcpOriginal],
        false,
    )
    .unwrap();
    let first = codec.encode_for(peer, b"data").unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].packet, b"\x04\0\0\0noise");
    assert_eq!(first[0].delay, Duration::from_millis(3));
    assert!(!first[0].payload);
    let mut inner = Codec::new(&[Mask::Wireguard, Mask::MkcpOriginal], true).unwrap();
    assert_eq!(inner.decode(&first[1].packet).unwrap(), b"data");
    tokio::time::advance(Duration::from_secs(20)).await;
    codec.finish_noise(peer);
    assert_eq!(codec.encode_for(peer, b"data").unwrap().len(), 1);
    assert_eq!(codec.encode_for(other, b"data").unwrap().len(), 2);
    codec.finish_noise(other);
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(codec.encode_for(peer, b"data").unwrap().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn noise_keeps_zero_reset_entries_and_rejects_unbounded_new_peers() {
    let mut config = settings();
    config.reset_seconds = Range::default();
    let mut state = State::new(&config).unwrap();
    for port in 1..=4096 {
        let peer = SocketAddr::from(([127, 0, 0, 1], port));
        assert_eq!(state.before(peer).unwrap().len(), 1);
        state.finish(peer);
    }
    tokio::time::advance(Duration::from_secs(86400)).await;
    assert!(state.before("127.0.0.1:4097".parse().unwrap()).is_err());
    assert!(state
        .before("127.0.0.1:1".parse().unwrap())
        .unwrap()
        .is_empty());
}
#[tokio::test]
async fn quic_noise_socket_sends_prelude_once_and_cancels_worker_on_drop() {
    use quinn::{udp::Transmit, Runtime};
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let raw = quinn::TokioRuntime.wrap_udp_socket(socket).unwrap();
    let weak = std::sync::Arc::downgrade(&raw);
    let masked = crate::finalmask::Socket::wrap(raw, &[Mask::Noise(settings())], false).unwrap();
    let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = receiver.local_addr().unwrap();
    for packet in [b"first".as_slice(), b"second".as_slice()] {
        masked
            .try_send(&Transmit {
                destination: peer,
                contents: packet,
                ecn: None,
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = [0; 100];
        for packet in [
            b"noise".as_slice(),
            b"first".as_slice(),
            b"second".as_slice(),
        ] {
            let n = receiver.recv(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..n], packet);
        }
    })
    .await
    .unwrap();
    drop(masked);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(weak.upgrade().is_none());
}
