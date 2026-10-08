use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use zero_traits::PacketBuffer;

struct Owner {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl AsMut<[u8]> for Owner {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
impl zero_traits::PacketStorage for Owner {}
impl Drop for Owner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn external_packet_survives_queue_and_unfragmented_stack_without_copy() {
    let drops = Arc::new(AtomicUsize::new(0));
    let data = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"owned",
    );
    let pointer = data.as_ptr() as usize;
    let packet = PacketBuffer::from_owner(Owner {
        data,
        drops: drops.clone(),
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    tx.send(packet).await.unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let mut fragments = zero_stack::FragmentReassembler::new();
    let zero_stack::OwnedFragmentOutcome::Packet {
        packet,
        reassembled: false,
    } = fragments.process_buffer(rx.recv().await.unwrap(), std::time::Instant::now())
    else {
        panic!("expected unchanged packet")
    };
    let mut packets = zero_stack::packet::fragment_ip_packet_buffer(packet, 1500, 1);
    let mut packet = packets.pop().unwrap();
    assert!(packets.is_empty());
    assert_eq!(packet.as_ptr() as usize, pointer);
    assert!(zero_stack::packet::advance_ip_hop(&mut packet));
    assert_eq!(zero_stack::packet::ip_hop_limit(&packet), Some(63));
    drop(packet);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn closed_or_full_queue_returns_the_owner_and_cancelled_queue_drops_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let new = || {
        PacketBuffer::from_owner(Owner {
            data: vec![1; 32],
            drops: drops.clone(),
        })
    };
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.try_send(new()).unwrap();
    let rejected = tx.try_send(new()).unwrap_err().into_inner();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(rejected);
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    let rejected = tx.try_send(new()).unwrap_err().into_inner();
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(rejected);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

#[test]
fn external_packet_fragmentation_releases_original_and_reassembles_exact_payload() {
    let drops = Arc::new(AtomicUsize::new(0));
    let bytes = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        &[0x5a; 1600],
    );
    let expected = bytes.clone();
    let packet = PacketBuffer::from_owner(Owner {
        data: bytes,
        drops: drops.clone(),
    });
    let fragments = zero_stack::packet::fragment_ip_packet_buffer(packet, 576, 5);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(fragments.len() > 1);
    let mut reassembly = zero_stack::FragmentReassembler::new();
    let mut recovered = None;
    for fragment in fragments {
        if let zero_stack::OwnedFragmentOutcome::Packet {
            packet,
            reassembled: true,
        } = reassembly.process_buffer(fragment, std::time::Instant::now())
        {
            recovered = Some(packet);
        }
    }
    let recovered = recovered.unwrap();
    assert_eq!(
        zero_stack::packet::ip_source(&recovered),
        zero_stack::packet::ip_source(&expected)
    );
    assert_eq!(
        zero_stack::packet::ip_destination(&recovered),
        zero_stack::packet::ip_destination(&expected)
    );
    assert_eq!(
        zero_stack::packet::parse_udp(&recovered).unwrap().payload,
        zero_stack::packet::parse_udp(&expected).unwrap().payload
    );
}
