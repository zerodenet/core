use super::*;
use std::borrow::Cow;
use std::sync::Arc;
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
#[test]
fn actual_native_one_frame_remains_held_while_passive_crypto_uses_same_roots_cpu_metadata() {
    let datagram = crate::quic_initial::tests::initial_packet();
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, datagram.len()).unwrap();
    let initial = root.driver_metadata().occupied;
    let read = root.native_grant(datagram.len()).unwrap();
    let mut native = read.try_buffer().unwrap();
    native.as_mut_bytes().copy_from_slice(&datagram);
    let original = native
        .into_packet(datagram.len())
        .unwrap_or_else(|_| panic!("same original datagram"));
    let pointer = original.as_ptr();
    let occupied = root.native();
    let view = read.memory_view().unwrap().driver_metadata_view().unwrap();
    let mut sniffer = QuicInitialSniffer::with_metadata(view).unwrap();
    let observation = sniffer.observe_prepaid(&original).unwrap();
    let QuicInitialObservation::ClientHello(Cow::Borrowed(hello)) = observation else {
        panic!("actual Source must borrow its paid reassembly")
    };
    assert_eq!(hello.len(), 241);
    assert!(hello.windows(11).any(|value| value == b"example.com"));
    assert_eq!(original.as_ptr(), pointer);
    assert_eq!(root.native(), occupied);
    assert!(root.driver_metadata().occupied.bytes() > initial.bytes());
    drop(sniffer);
    drop(original);
    read.revoke();
    drop(read);
    assert_eq!(root.driver_metadata().occupied, initial);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
#[test]
fn cpu_metadata_refusal_keeps_same_native_original_and_can_retry_without_a_second_frame() {
    let datagram = crate::quic_initial::tests::initial_packet();
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, datagram.len()).unwrap();
    let read = root.native_grant(datagram.len()).unwrap();
    let mut native = read.try_buffer().unwrap();
    native.as_mut_bytes().copy_from_slice(&datagram);
    let original = native
        .into_packet(datagram.len())
        .unwrap_or_else(|_| panic!("same actual original"));
    let mut sniffer = QuicInitialSniffer::with_metadata(
        read.memory_view().unwrap().driver_metadata_view().unwrap(),
    )
    .unwrap();
    let metadata = root.driver_metadata();
    let limit = metadata
        .occupied
        .checked_add(PacketMetadataLayout::new(1, 1))
        .unwrap();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        limit,
    )
    .unwrap();
    let before = root.native();
    assert!(sniffer.observe_prepaid(&original).is_err());
    assert_eq!(original.as_ref(), datagram);
    assert_eq!(root.native(), before);
    assert!(sniffer.source.as_ref().unwrap().value.numbers.is_empty());
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        metadata.limit,
    )
    .unwrap();
    assert!(matches!(
        sniffer.observe_prepaid(&original).unwrap(),
        QuicInitialObservation::ClientHello(Cow::Borrowed(_))
    ));
    assert_eq!(root.native(), before);
    drop(sniffer);
    drop(original);
    read.revoke();
}
#[test]
fn partial_crypto_row_growth_retains_old_bytes_and_retries_missing_presence_on_same_root() {
    let root = Arc::new(PacketReturnTotals::default());
    let before = root.driver_metadata();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        before
            .occupied
            .checked_add(PacketMetadataLayout::new(1, 1))
            .unwrap(),
    )
    .unwrap();
    let mut crypto = CryptoReassembler::source(root.driver_metadata_view());
    assert!(frame::collect_crypto_frames(&[6, 0, 1, 1], &mut crypto).is_err());
    assert!(crypto.client_hello_ref().unwrap().is_none());
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        before.limit,
    )
    .unwrap();
    frame::collect_crypto_frames(&[6, 0, 4, 1, 0, 0, 0], &mut crypto).unwrap();
    assert_eq!(crypto.client_hello_ref().unwrap(), Some(&[1, 0, 0, 0][..]));
    drop(crypto);
    assert_eq!(root.driver_metadata().occupied, before.occupied);
}
struct HeldCpuStorage {
    _bytes: Box<[u8]>,
    _credit: PacketMetadataCredit,
}
#[tokio::test]
async fn transient_real_cpu_storage_pressure_retains_original_across_pending_cancel_and_same_source_retry(
) {
    use std::future::Future;
    let datagram = crate::quic_initial::tests::initial_packet();
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, datagram.len()).unwrap();
    let read = root.native_grant(datagram.len()).unwrap();
    let mut buffer = read.try_buffer().unwrap();
    buffer.as_mut_bytes().copy_from_slice(&datagram);
    let original = buffer
        .into_packet(datagram.len())
        .unwrap_or_else(|_| panic!("actual original"));
    let memory = read.memory_view().unwrap().driver_metadata_view().unwrap();
    let mut sniffer = QuicInitialSniffer::with_metadata(memory.clone()).unwrap();
    let before = root.driver_metadata();
    let limit = before
        .occupied
        .checked_add(PacketMetadataLayout::new(64, 8192))
        .unwrap();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        limit,
    )
    .unwrap();
    let credit = memory
        .reserve(PacketMetadataLayout::array::<u8>(8192).unwrap())
        .unwrap();
    let held = HeldCpuStorage {
        _bytes: vec![0; 8192].into_boxed_slice(),
        _credit: credit,
    };
    let native = root.native();
    let pointer = original.as_ptr();
    {
        let work = sniffer.observe_prepaid_until(
            &original,
            tokio::time::Instant::now() + std::time::Duration::from_millis(200),
        );
        tokio::pin!(work);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(work.as_mut().poll(cx).is_pending()))
                .await
        );
        assert_eq!(root.native(), native);
        assert_eq!(original.as_ptr(), pointer);
        // Cancelling observation cannot erase its same original continuation
        // or authorize a second Native frame / provider or Scope close.
    }
    assert!(sniffer.source.as_ref().unwrap().value.inflight.is_some());
    assert_eq!(root.native(), native);
    drop(held); // actual CPU allocation deallocates before its matching refund
    let observation = sniffer
        .observe_prepaid_until(
            &original,
            tokio::time::Instant::now() + std::time::Duration::from_millis(200),
        )
        .await
        .unwrap();
    assert!(matches!(
        observation,
        QuicInitialObservation::ClientHello(Cow::Borrowed(_))
    ));
    assert_eq!(root.native(), native);
    assert_eq!(original.as_ptr(), pointer);
    drop(sniffer);
    drop(original);
    read.revoke();
}
