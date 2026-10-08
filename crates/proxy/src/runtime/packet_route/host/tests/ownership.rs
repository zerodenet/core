use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct PointerDevice {
    writes: Arc<AtomicUsize>,
    block: bool,
}
impl AsyncRead for PointerDevice {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for PointerDevice {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        packet: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes
            .store(packet.as_ptr() as usize, Ordering::Relaxed);
        if self.block {
            Poll::Pending
        } else {
            Poll::Ready(Ok(packet.len()))
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl zero_tun::TunDevice for PointerDevice {
    fn name(&self) -> &str {
        "host-test"
    }
    fn configure(&self, _: IpAddr, _: IpAddr, _: u16) -> io::Result<()> {
        panic!("must not configure")
    }
}

#[tokio::test]
async fn native_host_write_transfers_and_acknowledges_the_same_buffer() {
    let writes = Arc::new(AtomicUsize::new(0));
    let device = HostPacketDevice::start(
        PointerDevice {
            writes: writes.clone(),
            block: false,
        },
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap()],
        },
        1500,
    );
    device.activate();
    let buffer = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"owned",
    );
    let mut buffer = zero_traits::PacketBuffer::from_owner(buffer);
    let pointer = buffer.as_ptr() as usize;
    let (replies, _receiver) = mpsc::channel::<Vec<u8>>(1);
    let replies: zero_stack::packet_output::PacketSender = replies.into();
    device
        .forward(&mut buffer, 1, replies, 0, None)
        .await
        .unwrap();
    assert_eq!(writes.load(Ordering::Relaxed), pointer);
    assert_eq!(buffer.as_ptr() as usize, pointer);
    assert_eq!(packet::ip_hop_limit(&buffer), Some(63));
    assert_eq!(packet::parse_udp(&buffer).unwrap().payload, b"owned");
    // Rejected admission restores the valid original packet for fallback.
    let original = buffer.to_vec();
    let (conflicting, _receiver) = mpsc::channel::<Vec<u8>>(1);
    let conflicting: zero_stack::packet_output::PacketSender = conflicting.into();
    assert_eq!(
        device
            .forward(&mut buffer, 2, conflicting, 0, None)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AddrInUse
    );
    assert_eq!(buffer, original);
    assert_eq!(buffer.as_ptr() as usize, pointer);
    device.close();
    device.wait_stopped().await;
}

#[tokio::test]
async fn rejected_invalid_header_is_not_repaired_by_rollback() {
    let device = HostPacketDevice::start(
        PointerDevice {
            writes: Arc::new(AtomicUsize::new(0)),
            block: false,
        },
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec![],
        },
        1500,
    );
    device.activate();
    let mut packet = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"invalid",
    );
    packet[10] ^= 1;
    let expected = packet.clone();
    let mut packet: zero_traits::PacketBuffer = packet.into();
    let (replies, _receiver) = mpsc::channel::<Vec<u8>>(1);
    let replies: zero_stack::packet_output::PacketSender = replies.into();
    assert_eq!(
        device
            .forward(&mut packet, 1, replies, 0, None)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(packet, expected);
    device.close();
    device.wait_stopped().await;
}

#[tokio::test]
async fn host_shutdown_returns_an_unaccepted_buffer_for_fallback() {
    let writes = Arc::new(AtomicUsize::new(0));
    let device = HostPacketDevice::start(
        PointerDevice {
            writes: writes.clone(),
            block: true,
        },
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec![],
        },
        1500,
    );
    device.activate();
    let original = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"not accepted",
    );
    let mut buffer: zero_traits::PacketBuffer = original.clone().into();
    let pointer = buffer.as_ptr() as usize;
    let (replies, _receiver) = mpsc::channel::<Vec<u8>>(1);
    let replies: zero_stack::packet_output::PacketSender = replies.into();
    let sender = device.clone();
    let forwarding = tokio::spawn(async move {
        let result = sender.forward(&mut buffer, 1, replies, 0, None).await;
        (buffer, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while writes.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    device.close();
    let (buffer, result) = tokio::time::timeout(std::time::Duration::from_secs(1), forwarding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(buffer, original);
    assert_eq!(buffer.as_ptr() as usize, pointer);
    device.wait_stopped().await;
}
