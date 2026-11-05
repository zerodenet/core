use super::*;
use std::{
    io,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct View(tokio::io::DuplexStream);
impl AsyncRead for View {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl AsyncWrite for View {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
impl zero_tun::TunDevice for View {
    fn name(&self) -> &str {
        "host-test"
    }
    fn configure(&self, _: IpAddr, _: IpAddr, _: u16) -> io::Result<()> {
        panic!("borrowed view must not configure host")
    }
}
#[tokio::test]
async fn failed_sync_publication_stops_activated_fresh_but_keeps_borrowed_current() {
    for fresh in [true, false] {
        let (view, _host) = tokio::io::duplex(4096);
        let device = HostPacketDevice::start(View(view), config(9), 1500);
        let adapter = DirectAdapter::default();
        *adapter.packet.lock().unwrap() = Some(device.clone());
        if !fresh {
            device.activate();
        }
        let mut candidate = Prepared {
            next: Some(device.clone()),
            fresh,
            published: false,
        };
        candidate.publish_sync();
        assert!(
            device.usable(),
            "actual driver is active before commit outcome"
        );
        drop(candidate);
        if fresh {
            assert!(
                !device.usable(),
                "failed commit must fence an activated fresh original"
            );
            // This fixture supplies no native receipt. Cleanup must drive the
            // original join and retain Unsupported, never skip as healthy or
            // manufacture a native proof from its real worker join.
            let zero_engine::EngineError::Io(error) =
                adapter.close_discarded_device().await.unwrap_err()
            else {
                panic!("native proof is independent")
            };
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(adapter.packet.lock().unwrap().is_some());
        } else {
            adapter.close_discarded_device().await.unwrap();
            assert!(
                device.usable(),
                "borrowed current has no candidate cleanup right"
            );
            device.close();
        }
        device.wait_workers_stopped().await.unwrap();
    }
}
