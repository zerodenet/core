use std::io;
use std::net::{IpAddr, Ipv4Addr};

use super::ipv4_peer;

#[test]
fn selects_the_next_address_as_the_point_to_point_peer() {
    assert_eq!(
        ipv4_peer(
            Ipv4Addr::new(10, 66, 0, 1),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)),
        )
        .unwrap(),
        Ipv4Addr::new(10, 66, 0, 2)
    );
}

#[test]
fn selects_the_previous_address_at_the_end_of_the_subnet() {
    assert_eq!(
        ipv4_peer(
            Ipv4Addr::new(10, 66, 0, 255),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)),
        )
        .unwrap(),
        Ipv4Addr::new(10, 66, 0, 254)
    );
}

#[test]
fn rejects_an_ipv4_host_route_without_a_peer() {
    let error = ipv4_peer(
        Ipv4Addr::new(10, 66, 0, 1),
        IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn owned_utun_descriptor_preserves_framing_and_closes_on_drop() {
    use std::io::{Read, Write};
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (socket, mut peer) = UnixStream::pair().unwrap();
    socket.set_nonblocking(true).unwrap();
    let socket: OwnedFd = socket.into();
    // SAFETY: AsyncFd exclusively owns the valid, stable socket descriptor.
    let fd = unsafe { tokio::io::unix::AsyncFd::register(socket) }.unwrap();
    let mut device = super::Utun {
        name: "test-utun".into(),
        owned: crate::OwnedTunIdentity::from_fd(fd.get_ref().as_raw_fd()),
        fd,
        read_buffer: vec![0; 65_540],
        read_start: 0,
        read_end: 0,
    };
    let packet = [0x45, 0, 0, 4];
    let mut frame = (libc::AF_INET as u32).to_be_bytes().to_vec();
    frame.extend(packet);
    peer.write_all(&frame).unwrap();
    let mut received = [0; 4];
    device.read_exact(&mut received).await.unwrap();
    assert_eq!(received, packet);

    device.write_all(&packet).await.unwrap();
    let mut returned = [0; 8];
    peer.read_exact(&mut returned).unwrap();
    assert_eq!(returned.as_slice(), frame);
    drop(device);
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
}
