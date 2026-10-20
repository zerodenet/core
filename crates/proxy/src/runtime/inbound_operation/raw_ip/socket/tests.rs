use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll, time::Duration};
use zero_platform_tokio::{
    contract::component::GrantedDatagramListener,
    controlled_datagram::ControlledDatagramOwner,
};

#[tokio::test]
async fn granted_packet_socket_keeps_actual_source_pending_read_and_native_close_owner() {
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let (rx, tx, mut control) = owner
        .bind_listener(
            "127.0.0.1:0".parse().unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let address = rx.local_addr().unwrap();
    let (mut receive, mut send) = RawIpReceive::split(
        BoundInbound::ControlledDatagram(GrantedDatagramListener { rx, tx }),
        EgressInterfaceControl::default(),
    )
    .unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let remote = peer.local_addr().unwrap();
    peer.send_to(b"original packet", address).await.unwrap();
    let mut bytes = [0; 32];
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(2), receive.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&bytes[..size], b"original packet");
    assert_eq!(source, remote);
    send.send_to(b"reply", source).await.unwrap();
    let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"reply");
    assert_eq!(source, address);
    let pending = receive.recv_from(&mut bytes);
    tokio::pin!(pending);
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    control.revoke();
    assert!(pending.await.is_err());
    assert!(tokio::net::UdpSocket::bind(address).await.is_err());
    control.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
    let rebound = tokio::net::UdpSocket::bind(address).await.unwrap();
    assert_eq!(rebound.local_addr().unwrap(), address);
}
