use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_tun::TunDevice;

struct TestTun(tokio::io::DuplexStream);

struct RecordingTun {
    io: tokio::io::DuplexStream,
    configured: Arc<Mutex<Vec<(IpAddr, IpAddr, u16)>>>,
}

impl AsyncRead for TestTun {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TestTun {
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

impl TunDevice for TestTun {
    fn configure(&self, _addr: IpAddr, _mask: IpAddr, _mtu: u16) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "test-tun"
    }
}

impl AsyncRead for RecordingTun {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for RecordingTun {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl TunDevice for RecordingTun {
    fn configure(&self, addr: IpAddr, mask: IpAddr, mtu: u16) -> io::Result<()> {
        self.configured.lock().unwrap().push((addr, mask, mtu));
        Ok(())
    }

    fn name(&self) -> &str {
        "recording-tun"
    }
}

#[test]
fn default_device_configuration_applies_both_address_families() {
    let (io, _peer) = tokio::io::duplex(64);
    let configured = Arc::new(Mutex::new(Vec::new()));
    let device = RecordingTun {
        io,
        configured: Arc::clone(&configured),
    };
    let addresses = [
        (
            "10.66.0.1".parse().unwrap(),
            "255.255.255.0".parse().unwrap(),
        ),
        (
            "fd66::1".parse().unwrap(),
            "ffff:ffff:ffff:ffff::".parse().unwrap(),
        ),
    ];

    device.configure_addresses(&addresses, 1400).unwrap();

    let applied = configured.lock().unwrap();
    assert_eq!(applied.len(), 2);
    assert!(applied[0].0.is_ipv4());
    assert!(applied[1].0.is_ipv6());
    assert!(applied.iter().all(|(_, _, mtu)| *mtu == 1400));
}

#[tokio::test]
async fn channel_writer_progresses_while_device_read_is_pending() {
    let (device, mut peer) = tokio::io::duplex(1024);
    let (writer, mut reader) = TestTun(device).into_channels().expect("split test TUN");

    writer
        .send(vec![1, 2, 3, 4])
        .await
        .expect("queue outbound packet");
    let mut outbound = [0_u8; 4];
    tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut outbound))
        .await
        .expect("outbound write was blocked by pending read")
        .expect("read outbound packet");
    assert_eq!(outbound, [1, 2, 3, 4]);

    peer.write_all(&[5, 6, 7])
        .await
        .expect("write inbound packet");
    let inbound = tokio::time::timeout(Duration::from_secs(1), reader.recv())
        .await
        .expect("inbound read timed out")
        .expect("inbound channel closed");
    assert_eq!(inbound, [5, 6, 7]);

    drop(writer);
}
