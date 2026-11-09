//! Actual Root quota, pending admission, MTU and final output custody.
use super::*;

struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn endpoints() -> (SocketAddress, SocketAddress) {
    (
        SocketAddress::new(zero_traits::IpAddress::V4([203, 0, 113, 8]), 53),
        SocketAddress::new(zero_traits::IpAddress::V4([10, 0, 0, 2]), 12000),
    )
}
#[tokio::test]
async fn full_actual_transmit_root_preserves_input_until_original_credit_is_returned() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_transmit(1, 32).unwrap();
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let payload = view.copy_received_packet(b"same").unwrap();
    let held = root.transmit_packet(32, 0, 0).unwrap();
    let (send, mut output) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let stack = UserUdpStack::new_output(
        PacketSender::from(send)
            .with_packet_memory_view(view, PacketWireMemory::Native)
            .unwrap(),
        1500,
    );
    let (source, destination) = endpoints();
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    {
        let send = stack.send_to(&payload, source, destination);
        tokio::pin!(send);
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert_eq!(payload.as_ref(), b"same");
        assert_eq!(
            root.transmit().occupied,
            PacketMemoryLimit {
                frames: 1,
                bytes: 32
            }
        );
        assert_eq!(root.native().occupied.bytes, 68);
        assert!(output.try_recv().is_err());
        drop(held);
        assert!(
            count.0.load(Ordering::SeqCst) > 0,
            "actual blocked admission must be woken"
        );
        send.as_mut().await;
    }
    let packet = output.try_recv().unwrap();
    let parsed = packet::parse_udp(&packet).unwrap();
    assert_eq!(parsed.payload, b"same");
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 32
        }
    );
    drop(payload);
    assert_eq!(root.native().occupied.bytes, 64);
    drop(packet);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}

#[tokio::test]
async fn rejected_new_feedback_cannot_evict_held_return_and_reuses_exact_original_layout_after_refund(
) {
    let root = Arc::new(PacketReturnTotals::default());
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let payload = [9; 180];
    let (source, destination) = endpoints();
    let original_image = input(&payload);
    let expected = packet::build_udp_unreachable_response(&original_image, 100).unwrap();
    let encoded = PacketWireMemory::PaddedEnvelope {
        multiple: 16,
        overhead: 32,
    }
    .workspace_lengths(expected.len())
    .unwrap();
    let frames = 2 + encoded.len();
    let bytes = original_image.len() + expected.len() + encoded.iter().sum::<usize>();
    root.install_transmit(frames, bytes).unwrap();
    let held = root.transmit_packet(bytes, frames - 1, 0).unwrap();
    let (send, mut output) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let stack = UserUdpStack::new_output(
        PacketSender::from(send)
            .with_packet_memory_view(
                view,
                PacketWireMemory::PaddedEnvelope {
                    multiple: 16,
                    overhead: 32,
                },
            )
            .unwrap(),
        100,
    );
    assert!(!stack.send_unreachable(&payload, destination, source));
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit { frames, bytes }
    );
    assert!(output.try_recv().is_err());
    assert_eq!(held.len(), bytes);
    drop(held);
    assert!(stack.send_unreachable(&payload, destination, source));
    let feedback = output.try_recv().unwrap();
    assert_eq!(feedback.as_ref(), expected.as_slice());
    assert!(feedback.len() <= 100);
    // Original+response+actual codec promises stay charged together until the
    // real response, not just the temporary original image, is returned.
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit { frames, bytes }
    );
    drop(feedback);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}

#[tokio::test]
async fn actual_revoked_return_wait_releases_its_input_only_after_the_same_original_fence() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_transmit(1, 32).unwrap();
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let payload = view.copy_received_packet(b"stop").unwrap();
    let held = root.transmit_packet(32, 0, 0).unwrap();
    let (send, mut output) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let stack = UserUdpStack::new_output(
        PacketSender::from(send)
            .with_packet_memory_view(view, PacketWireMemory::Native)
            .unwrap(),
        1500,
    );
    let (source, destination) = endpoints();
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    {
        let send = stack.send_to(&payload, source, destination);
        tokio::pin!(send);
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        original.revoke();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        send.as_mut().await;
    }
    assert!(output.try_recv().is_err());
    assert_eq!(payload.as_ref(), b"stop");
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 4
        }
    );
    drop(payload);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    assert_eq!(
        root.transmit().occupied.frames,
        1,
        "another actual owner is not refunded by cancellation"
    );
    drop(held);
    assert_eq!(root.transmit().occupied.frames, 0);
}
