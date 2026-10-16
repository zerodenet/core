use super::*;
use std::{ffi::CString, sync::Arc, time::Duration};
use tokio::io::Interest;

async fn loopback_interface_roundtrip(address: &str, specific: bool) {
    let name = if cfg!(target_os = "macos") {
        "lo0"
    } else {
        "lo"
    };
    let index = unsafe { libc::if_nametoindex(CString::new(name).unwrap().as_ptr()) };
    let interface = EgressInterface::new(name, index).unwrap();
    let listen = if specific {
        address
    } else if address.starts_with('[') {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    };
    let listener = Arc::new(tokio::net::UdpSocket::bind(listen).await.unwrap());
    let original = listener.local_addr().unwrap();
    let peer = tokio::net::UdpSocket::bind(address).await.unwrap();
    let target = peer.local_addr().unwrap();
    for interface in [Some(&interface), None, Some(&interface)] {
        listener
            .async_io(Interest::WRITABLE, || {
                send(&listener, b"hint", target, interface)
            })
            .await
            .unwrap();
        let mut bytes = [0; 8];
        let (size, source) =
            tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"hint");
        assert_eq!(source.port(), original.port());
        assert_eq!(listener.local_addr().unwrap(), original);
        peer.send_to(b"return", source).await.unwrap();
        let (size, source) =
            tokio::time::timeout(Duration::from_secs(1), listener.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"return");
        assert_eq!(source, target);
    }
    #[cfg(target_os = "macos")]
    {
        // A transmit hint must never leave IP_BOUND_IF/IPV6_BOUND_IF on the
        // shared receive socket, including after alternating hinted sends.
        use std::os::fd::AsRawFd;
        let mut index = u32::MAX;
        let mut size = std::mem::size_of::<u32>() as libc::socklen_t;
        let (level, option) = if target.is_ipv6() {
            (libc::IPPROTO_IPV6, libc::IPV6_BOUND_IF)
        } else {
            (libc::IPPROTO_IP, libc::IP_BOUND_IF)
        };
        assert_eq!(
            unsafe {
                libc::getsockopt(
                    listener.as_raw_fd(),
                    level,
                    option,
                    (&mut index as *mut u32).cast(),
                    &mut size,
                )
            },
            0
        );
        assert_eq!(index, 0, "listener reception must remain unbound");
    }
}

#[tokio::test]
async fn ipv4_packet_hint_retains_port_and_does_not_bind_the_listener() {
    loopback_interface_roundtrip("127.0.0.1:0", false).await;
}

#[tokio::test]
async fn ipv6_packet_hint_retains_port_and_does_not_bind_the_listener() {
    loopback_interface_roundtrip("[::1]:0", false).await;
}

#[tokio::test]
async fn ipv4_packet_hint_retains_specific_receive_address_and_port() {
    loopback_interface_roundtrip("127.0.0.1:0", true).await;
}

#[tokio::test]
async fn ipv6_packet_hint_retains_specific_receive_address_and_port() {
    loopback_interface_roundtrip("[::1]:0", true).await;
}
