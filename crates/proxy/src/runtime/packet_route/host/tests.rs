use super::*;
use std::{
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct Device(tokio::io::DuplexStream);
impl AsyncRead for Device {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl AsyncWrite for Device {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
impl zero_tun::TunDevice for Device {
    fn name(&self) -> &str {
        "host-test"
    }
    fn configure(&self, _: IpAddr, _: IpAddr, _: u16) -> io::Result<()> {
        panic!("packet sink must never configure host")
    }
}
#[tokio::test]
async fn host_packet_sink_is_bidirectional_preserves_source_and_waits_for_device_writes() {
    let (io, mut host) = tokio::io::duplex(8192);
    let device = HostPacketDevice::start(
        Device(io),
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap()],
        },
        1500,
    );
    assert!(!device.usable());
    device.activate();
    let original = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"hello",
    );
    let (replies, mut rx) = mpsc::channel(8);
    device
        .forward(&mut original.clone(), 1, replies, 0, None)
        .await
        .unwrap();
    let mut outgoing = vec![0; original.len()];
    host.read_exact(&mut outgoing).await.unwrap();
    assert_eq!(packet::ip_source(&outgoing), packet::ip_source(&original));
    assert_eq!(
        packet::ip_hop_limit(&outgoing),
        packet::ip_hop_limit(&original).map(|v| v - 1)
    );
    let reply = packet::build_udp(
        "192.0.2.1".parse().unwrap(),
        "10.0.0.2".parse().unwrap(),
        443,
        40000,
        b"world",
    );
    host.write_all(&reply).await.unwrap();
    let returned = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(packet::parse_udp(&returned).unwrap().payload, b"world");
    device.close();
    tokio::time::timeout(std::time::Duration::from_secs(1), device.wait_stopped())
        .await
        .unwrap();
    assert!(!device.usable());
}
#[tokio::test]
async fn staged_host_device_does_not_read_before_publish_and_cancel_confirms_release() {
    let (io, mut host) = tokio::io::duplex(8192);
    let device = HostPacketDevice::start(
        Device(io),
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap()],
        },
        1500,
    );
    host.write_all(b"queued").await.unwrap();
    assert!(!device.usable());
    device.close();
    tokio::time::timeout(std::time::Duration::from_secs(1), device.wait_stopped())
        .await
        .unwrap();
}

#[tokio::test]
async fn direct_packet_sink_accepts_other_ip_protocols_and_uses_real_router_address_for_errors() {
    let (io, mut host) = tokio::io::duplex(8192);
    let device = HostPacketDevice::start(
        Device(io),
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap(), "fd64::1".parse().unwrap()],
        },
        1280,
    );
    device.activate();
    let mut original = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"opaque",
    );
    original[9] = 99;
    original[10..12].fill(0);
    let sum = packet::checksum(&original[..20]);
    original[10..12].copy_from_slice(&sum.to_be_bytes());
    let (replies, _rx) = mpsc::channel(8);
    let observed = device
        .forward(&mut original.clone(), 1, replies.clone(), 0, None)
        .await
        .unwrap();
    assert!(observed.response.is_none());
    let mut outgoing = vec![0; original.len()];
    host.read_exact(&mut outgoing).await.unwrap();
    assert_eq!(outgoing[9], 99);
    original[8] = 1;
    original[10..12].fill(0);
    let sum = packet::checksum(&original[..20]);
    original[10..12].copy_from_slice(&sum.to_be_bytes());
    let response = device
        .forward(&mut original.clone(), 1, replies.clone(), 0, None)
        .await
        .unwrap()
        .response
        .unwrap();
    assert_eq!(
        packet::ip_source(&response),
        Some("10.64.0.1".parse().unwrap())
    );
    assert_eq!(response[20], 11);
    assert_eq!(packet::checksum(&response[..20]), 0);
    let large = packet::build_udp(
        "fd64::2".parse().unwrap(),
        "fd64::3".parse().unwrap(),
        40000,
        443,
        &vec![0; 1400],
    );
    let response = device
        .forward(&mut large.clone(), 1, replies, 0, None)
        .await
        .unwrap()
        .response
        .unwrap();
    assert_eq!(
        packet::ip_source(&response),
        Some("fd64::1".parse().unwrap())
    );
    assert!(packet::parse_icmp_error(&response).is_some());
    device.close();
    device.wait_stopped().await;
}

#[tokio::test]
async fn closing_a_blocked_host_write_preserves_the_shared_device_and_other_paths() {
    let (io, mut host) = tokio::io::duplex(8192);
    let device = HostPacketDevice::start(
        Device(io),
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap()],
        },
        9000,
    );
    device.activate();
    let (replies, mut rx) = mpsc::channel(8);
    let first = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        &vec![0; 8164],
    );
    device
        .forward(&mut first.clone(), 1, replies.clone(), 0, None)
        .await
        .unwrap();
    let next = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40001,
        443,
        b"next",
    );
    let sender = device.clone();
    let mut request = next.clone();
    let pointer = request.as_ptr() as usize;
    let waiting = tokio::spawn(async move {
        let result = sender.forward(&mut request, 1, replies, 0, None).await;
        (request, result)
    });
    tokio::task::yield_now().await;
    rx.close();
    let (restored, result) = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.err().unwrap().kind(), io::ErrorKind::Interrupted);
    assert_eq!(restored, next);
    assert_eq!(restored.as_ptr() as usize, pointer);
    assert!(device.usable());
    host.read_exact(&mut vec![0; 8192]).await.unwrap();
    let (other, _other_rx) = mpsc::channel(8);
    device
        .forward(&mut next.clone(), 1, other, 0, None)
        .await
        .unwrap();
    host.read_exact(&mut vec![0; next.len()]).await.unwrap();
    device.close();
    device.wait_stopped().await;
}

#[tokio::test]
async fn fragmented_host_returns_preserve_delivery_and_declare_incomplete_role_measurement() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug, Default)]
    struct Coverage(AtomicUsize);
    impl IoObserver for Coverage {
        fn receive_coverage_lost(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn received(&self, _: usize) {}
        fn sent(&self, _: usize) {}
        fn error(&self) {}
        fn dropped(&self) {}
    }
    let (io, mut host) = tokio::io::duplex(8192);
    let device = HostPacketDevice::start(
        Device(io),
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap()],
        },
        1500,
    );
    device.activate();
    let coverage = Arc::new(Coverage::default());
    let (replies, mut received) = mpsc::channel(8);
    let request = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        b"q",
    );
    device
        .forward(&mut request.clone(), 1, replies, 0, Some(coverage.clone()))
        .await
        .unwrap();
    host.read_exact(&mut vec![0; request.len()]).await.unwrap();
    let response = packet::build_udp(
        "192.0.2.1".parse().unwrap(),
        "10.0.0.2".parse().unwrap(),
        443,
        40000,
        &vec![3; 1500],
    );
    let fragments = packet::fragment_ip_packet(&response, 1280, 7);
    assert_eq!(fragments.len(), 2);
    host.write_all(&fragments[0]).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while coverage.0.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    host.write_all(&fragments[1]).await.unwrap();
    let returned = tokio::time::timeout(std::time::Duration::from_secs(1), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        packet::parse_udp(&returned).unwrap().payload,
        &vec![3; 1500]
    );
    assert_eq!(coverage.0.load(Ordering::Relaxed), 1);
    device.close();
    device.wait_stopped().await;
}

#[path = "tests/ownership.rs"]
mod ownership;
