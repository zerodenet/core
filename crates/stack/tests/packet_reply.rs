use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use zero_stack::packet_output::{ObservedPacket, PacketSender};
use zero_traits::{IoObserver, PacketBuffer, PacketStorage, TcpStack, UdpStack};

struct Owner {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
    conversions: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Owner {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl PacketStorage for Owner {
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        self.conversions.fetch_add(1, Ordering::Relaxed);
        self.bytes.clone()
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}
fn owner(drops: &Arc<AtomicUsize>, conversions: &Arc<AtomicUsize>) -> PacketBuffer {
    PacketBuffer::from_owner(Owner {
        bytes: vec![0x5a; 148],
        drops: drops.clone(),
        conversions: conversions.clone(),
    })
}

#[tokio::test]
async fn owned_output_and_weak_sender_preserve_storage_until_receiver_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let conversions = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel::<PacketBuffer>(1);
    let output = PacketSender::from(tx);
    let weak = output.downgrade();
    let packet = owner(&drops, &conversions);
    let pointer = packet.as_ptr();
    output.try_send_buffer(packet).unwrap();
    drop(output);
    assert!(weak.upgrade().is_none());
    let mut packet = rx.recv().await.unwrap();
    assert_eq!(packet.as_ptr(), pointer);
    packet[0] = 7;
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(conversions.load(Ordering::Relaxed), 0);
    drop(packet);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[derive(Debug)]
struct Observer;
impl IoObserver for Observer {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}

#[tokio::test]
async fn compatibility_conversion_requires_capacity_and_preserves_observation() {
    let drops = Arc::new(AtomicUsize::new(0));
    let conversions = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
    let output = PacketSender::from(tx);
    output.try_send(vec![1]).unwrap();
    let packet = owner(&drops, &conversions);
    let pointer = packet.as_ptr();
    let packet = output.try_send_buffer(packet).unwrap_err().into_inner();
    assert_eq!(packet.as_ptr(), pointer);
    assert_eq!(conversions.load(Ordering::Relaxed), 0);
    rx.recv().await.unwrap();
    output.send_buffer(packet).await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), vec![0x5a; 148]);
    assert_eq!(conversions.load(Ordering::Relaxed), 1);
    drop(rx);
    let packet = owner(&drops, &conversions);
    let packet = output.send_buffer(packet).await.unwrap_err().0;
    assert_eq!(conversions.load(Ordering::Relaxed), 1);
    drop(packet);
    let observer: Arc<dyn IoObserver> = Arc::new(Observer);
    let (tx, mut rx) = mpsc::channel::<ObservedPacket>(1);
    let output = PacketSender::from(tx).with_observer(Some(observer.clone()));
    output.try_send_buffer(owner(&drops, &conversions)).unwrap();
    let packet = rx.recv().await.unwrap();
    assert!(Arc::ptr_eq(packet.observer.as_ref().unwrap(), &observer));
    assert_eq!(packet.packet, vec![0x5a; 148]);
    assert_eq!(conversions.load(Ordering::Relaxed), 2);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn cancelled_full_output_releases_owner_once_without_conversion() {
    for legacy in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let conversions = Arc::new(AtomicUsize::new(0));
        let (plain, plain_rx) = mpsc::channel::<Vec<u8>>(1);
        let (owned, owned_rx) = mpsc::channel::<PacketBuffer>(1);
        let output = if legacy {
            PacketSender::from(plain)
        } else {
            owned.into()
        };
        output.try_send(vec![0]).unwrap();
        let packet = owner(&drops, &conversions);
        let mut future = Box::pin(output.send_buffer(packet));
        tokio::select! { biased;
            result = future.as_mut() => panic!("full queue accepted packet: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        drop(future);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(plain_rx);
        drop(owned_rx);
        assert_eq!(conversions.load(Ordering::Relaxed), 0);
        assert!(output.is_closed());
        output.closed().await;
    }
}

#[tokio::test]
async fn user_stack_tcp_and_udp_responses_use_owned_output() {
    use zero_stack::{packet, UserNetworkStack};
    let (tx, mut rx) = mpsc::channel::<PacketBuffer>(8);
    let (tcp, udp) = UserNetworkStack::new_with_packet_output(tx.into(), 1440).into_parts();
    tcp.feed(&packet::build_tcp_with_mss(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.1".parse().unwrap(),
        40000,
        443,
        10,
        0,
        packet::tcp_flags::SYN,
        1440,
    ))
    .await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let parsed = packet::parse_tcp(&response).unwrap();
    assert!(parsed.syn && parsed.ack_flag);
    assert_eq!(parsed.ack, 11);
    let source = zero_traits::SocketAddress::new(zero_traits::IpAddress::V4([10, 0, 0, 1]), 53);
    let destination =
        zero_traits::SocketAddress::new(zero_traits::IpAddress::V4([10, 0, 0, 2]), 40001);
    udp.send_to(b"answer", source, destination).await;
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let response = rx.recv().await.unwrap();
            if packet::parse_udp(&response).is_some() {
                break response;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(packet::parse_udp(&response).unwrap().payload, b"answer");
}
