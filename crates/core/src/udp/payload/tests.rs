use super::*;
use core::sync::atomic::{AtomicUsize, Ordering};
struct Original {
    bytes: [u8; 3],
    dropped: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Original {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Original {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl zero_traits::PacketStorage for Original {}
impl Drop for Original {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn borrowed_core_dispatch_keeps_the_same_packet_owner_without_copying_or_exporting_a_vec() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let packet = zero_traits::PacketBuffer::from_owner(Original {
        bytes: [1, 2, 3],
        dropped: dropped.clone(),
    });
    let dispatch = InboundUdpDispatch::new_with_packet(
        ProtocolType::UNKNOWN,
        Address::Ipv4([1, 2, 3, 4]),
        53,
        packet,
        None,
    );
    assert_eq!(dispatch.payload(), &[1, 2, 3]);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(dispatch);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
