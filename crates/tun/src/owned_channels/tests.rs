use super::*;
use std::future::Future;
use std::{
    os::fd::AsRawFd,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct Device {
    io: tokio::net::UnixStream,
    identity: OwnedTunIdentity,
}
impl Device {
    fn pair() -> (Self, tokio::net::UnixStream) {
        let (io, peer) = tokio::net::UnixStream::pair().unwrap();
        let identity = OwnedTunIdentity::from_fd(io.as_raw_fd());
        (Self { io, identity }, peer)
    }
}
impl AsyncRead for Device {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for Device {
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
impl crate::TunDevice for Device {
    fn configure(&self, _: std::net::IpAddr, _: std::net::IpAddr, _: u16) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "real original retained after configuration failure",
        ))
    }
    fn name(&self) -> &str {
        "actual-channel-owned-descriptor"
    }
}
impl OwnedTunDevice for Device {
    fn owned_identity(&self) -> OwnedTunIdentity {
        self.identity.clone()
    }
    fn close_owned(self) -> io::Result<TunCloseReceipt> {
        crate::owned::close_fd(
            self.io.into_std()?,
            "actual-channel-owned-descriptor".into(),
            self.identity,
        )
    }
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
async fn configure_failure_keeps_the_same_original_for_explicit_close() {
    let (device, _peer) = Device::pair();
    let identity = device.owned_identity();
    let owner = OwnedTunChannels::new(device);
    let error = owner
        .configure_addresses(
            &[(
                "10.1.0.1".parse().unwrap(),
                "255.255.255.0".parse().unwrap(),
            )],
            1500,
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let receipt = owner.close().await.unwrap();
    assert!(receipt.identity().same_owner(&identity));
    assert!(Arc::ptr_eq(&receipt, &owner.close().await.unwrap()));
}
#[tokio::test]
async fn cancelled_close_keeps_actual_tasks_and_reunites_the_same_device() {
    let (device, mut peer) = Device::pair();
    let identity = device.owned_identity();
    let owner = OwnedTunChannels::new(device);
    let (writer, mut reader) = owner.start(Arc::new(Observer)).unwrap();
    peer.write_all(b"incoming").await.unwrap();
    assert_eq!(reader.recv().await.unwrap(), b"incoming");
    writer.send(b"outgoing".to_vec()).await.unwrap();
    let mut response = [0; 8];
    peer.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"outgoing");
    let read_id = owner.state.lock().await.reader.as_ref().unwrap().id();
    let mut close = Box::pin(owner.close());
    let pending =
        std::future::poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx).is_pending())).await;
    assert!(pending);
    drop(close);
    assert_eq!(
        owner.state.lock().await.reader.as_ref().unwrap().id(),
        read_id
    );
    let receipt = owner.close().await.unwrap();
    assert!(receipt.identity().same_owner(&identity));
    assert!(Arc::ptr_eq(&receipt, &owner.close().await.unwrap()));
}

#[tokio::test]
async fn actual_driver_io_failure_and_matching_native_close_are_both_retained() {
    let (device, peer) = Device::pair();
    let identity = device.owned_identity();
    let owner = OwnedTunChannels::new(device);
    let (writer, _reader) = owner.start(Arc::new(Observer)).unwrap();
    drop(peer);
    writer
        .send(b"write to closed actual peer".to_vec())
        .await
        .unwrap();
    writer.closed().await;
    for _ in 0..2 {
        let error = owner.close().await.unwrap_err();
        let failures = error
            .get_ref()
            .unwrap()
            .downcast_ref::<CloseFailures>()
            .unwrap();
        assert!(failures
            .0
            .iter()
            .any(|source| source.kind() == io::ErrorKind::BrokenPipe));
        let receipt = owner.closed_receipt().await.unwrap().unwrap();
        assert!(receipt.identity().same_owner(&identity));
    }
}
