use super::*;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Wake, Waker},
};
use zero_platform_tokio::controlled_carrier::{CarrierBufferLimits, CarrierOriginalTotals};
struct Reenter(Arc<Socket>, AtomicUsize);
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // A real refund must be outside the incoming receiver mutex as well
        // as both Root/lane locks. The callback may synchronously reenter.
        let _guard = self
            .0
            .incoming
            .try_lock()
            .expect("no queue mutex surrounds refunds");
        self.1.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn actual_packet_poller_reuses_its_working_ticket_when_root_has_no_free_bytes() {
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let minimum = memory::RECEIVE_BYTES * 2 + memory::MAX_DATAGRAM_BYTES;
    root.install_buffer_limits_after_retirement(CarrierBufferLimits {
        max_queue_packets: 2,
        total_max_bytes: minimum,
    })
    .unwrap();
    let book = root.buffer_budget();
    let logical = "127.0.0.1:443".parse().unwrap();
    let memory = memory::Memory::prepare(book.clone(), &[]).unwrap();
    let (socket, mut channels) = prepare_budgeted_channels(logical, logical, memory);
    let frame = Transmit {
        destination: logical,
        ecn: None,
        contents: b"same original FIFO",
        segment_size: None,
        src_ip: None,
    };
    socket.try_send(&frame).unwrap();
    assert_eq!(book.used_bytes(), minimum);
    assert_eq!(
        socket.try_send(&frame).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let wake = Arc::new(Reenter(socket.clone(), AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut poller = socket.clone().create_io_poller();
    assert!(poller.as_mut().poll_writable(&mut cx).is_pending());
    let first = channels.outgoing.try_recv().unwrap();
    assert_eq!(first.bytes, frame.contents);
    drop(first);
    assert_eq!(book.used_bytes(), minimum);
    assert!(wake.1.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poller.as_mut().poll_writable(&mut cx),
        Poll::Ready(Ok(()))
    ));
    socket.try_send(&frame).unwrap();
    let second = channels.outgoing.try_recv().unwrap();
    assert_eq!(second.bytes.as_slice(), frame.contents);
    drop(second);
    drop(poller);
    drop(waker);
    drop(wake);
    drop(channels);
    drop(socket);
    assert_eq!(book.used_bytes(), 0);
}
#[test]
fn stateless_mask_copy_peak_is_prepaid_before_any_queue_or_native_resource_exists() {
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    root.install_buffer_limits_after_retirement(CarrierBufferLimits {
        max_queue_packets: 2,
        total_max_bytes: memory::RECEIVE_BYTES * 2 + memory::MAX_DATAGRAM_BYTES,
    })
    .unwrap();
    let book = root.buffer_budget();
    let masks = [crate::finalmask::udp::Mask::Salamander {
        password: "fixture-only-mask".into(),
    }];
    assert!(memory::Memory::prepare(book.clone(), &masks).is_err());
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(root.snapshot().charged, 0);
}
