use super::*;
use std::ffi::CString;
use tokio::net::UdpSocket;

fn index(name: &str) -> u32 {
    unsafe { libc::if_nametoindex(CString::new(name).unwrap().as_ptr()) }
}
fn current(socket: &UdpSocket) -> u32 {
    let mut value = u32::MAX;
    let mut length = std::mem::size_of::<u32>() as libc::socklen_t;
    assert_eq!(
        unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_BOUND_IF,
                (&mut value as *mut u32).cast(),
                &mut length,
            )
        },
        0
    );
    value
}

#[tokio::test]
async fn send_failure_restores_specific_address_port_and_previous_scope() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let loopback = EgressInterface::new("lo0", index("lo0")).unwrap();
    // An oversized UDP payload is rejected synchronously. Restoration
    // must still happen before the error is exposed to the neutral runtime.
    let error = send(
        &socket,
        &vec![0; 65_536],
        "127.0.0.1:9".parse().unwrap(),
        &loopback,
    )
    .unwrap_err();
    assert_ne!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(socket.local_addr().unwrap(), local);
    assert_eq!(current(&socket), 0);

    bind_fd_to_index(socket.as_raw_fd(), false, loopback.index()).unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert_eq!(
        send(&socket, b"same", peer.local_addr().unwrap(), &loopback).unwrap(),
        4
    );
    assert_eq!(
        current(&socket),
        loopback.index(),
        "retain a pre-existing nonzero scope"
    );
}

#[tokio::test]
async fn failed_scope_selection_never_changes_specific_receive_binding() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let original = socket.local_addr().unwrap();
    assert!(Scope::enter(socket.as_raw_fd(), u32::MAX).is_err());
    assert_eq!(current(&socket), 0);
    assert_eq!(socket.local_addr().unwrap(), original);
}

#[tokio::test]
async fn scope_drop_restores_after_early_unwind() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let original = socket.local_addr().unwrap();
    let failed = std::panic::catch_unwind(|| {
        let _scope = Scope::enter(socket.as_raw_fd(), index("lo0")).unwrap();
        panic!("exercise synchronous scope unwind");
    });
    assert!(failed.is_err());
    assert_eq!(current(&socket), 0);
    assert_eq!(socket.local_addr().unwrap(), original);
}

#[tokio::test]
async fn scoped_transmit_does_not_restrict_loopback_receive_to_another_interface() {
    let Some(interface) = ["en0", "en1", "en5"]
        .into_iter()
        .map(index)
        .find(|value| *value != 0)
    else {
        return;
    };
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let scope = Scope::enter(socket.as_raw_fd(), interface).unwrap();
    peer.send_to(b"receive", socket.local_addr().unwrap())
        .await
        .unwrap();
    let mut bytes = [0; 16];
    // Deliberately receive while scoped: the real send path has no suspension.
    let (size, source) = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        socket.recv_from(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&bytes[..size], b"receive");
    assert_eq!(source, peer.local_addr().unwrap());
    scope.restore().unwrap();
    assert_eq!(current(&socket), 0);
}
