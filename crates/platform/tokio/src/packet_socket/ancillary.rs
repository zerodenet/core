//! Platform datagram egress preserving the shared receive address and port.
use crate::EgressInterface;
#[cfg(any(target_os = "macos", target_os = "linux", windows))]
use std::io::IoSlice;
use std::{io, net::SocketAddr};

#[cfg(target_os = "macos")]
mod macos;

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
pub(super) fn send(
    socket: &tokio::net::UdpSocket,
    bytes: &[u8],
    peer: SocketAddr,
    interface: Option<&EgressInterface>,
) -> io::Result<usize> {
    #[cfg(target_os = "macos")]
    match interface {
        Some(interface) if peer.is_ipv4() && !socket.local_addr()?.ip().is_unspecified() => {
            return macos::send(socket, bytes, peer, interface);
        }
        _ => {}
    }
    let socket = socket2::SockRef::from(socket);
    let address = socket2::SockAddr::from(peer);
    let buffers = [IoSlice::new(bytes)];
    let mut control = Control::new();
    if let Some(interface) = interface {
        control.interface(peer, interface);
    }
    let message = socket2::MsgHdr::new()
        .with_addr(&address)
        .with_buffers(&buffers);
    let message = if control.used == 0 {
        message
    } else {
        message.with_control(control.bytes())
    };
    socket.sendmsg(&message, 0)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub(super) fn send(
    socket: &tokio::net::UdpSocket,
    bytes: &[u8],
    peer: SocketAddr,
    interface: Option<&EgressInterface>,
) -> io::Result<usize> {
    if interface.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "per-datagram interface selection is unavailable",
        ));
    }
    socket.try_send_to(bytes, peer)
}

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
#[repr(align(16))]
struct Control {
    data: [u8; 128],
    used: usize,
}

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
impl Control {
    fn new() -> Self {
        Self {
            data: [0; 128],
            used: 0,
        }
    }
    fn bytes(&self) -> &[u8] {
        &self.data[..self.used]
    }

    #[cfg(unix)]
    fn push<T: Copy>(&mut self, level: i32, kind: i32, value: T) {
        // The fixed buffer is 16-byte aligned, offsets use CMSG_SPACE and each
        // value/header is fully initialized. It lives through sendmsg only.
        let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<T>() as _) } as usize;
        assert!(self.used + space <= self.data.len());
        unsafe {
            let header = self
                .data
                .as_mut_ptr()
                .add(self.used)
                .cast::<libc::cmsghdr>();
            (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<T>() as _) as _;
            (*header).cmsg_level = level;
            (*header).cmsg_type = kind;
            std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<T>(), value);
        }
        self.used += space;
    }

    #[cfg(windows)]
    fn push<T: Copy>(&mut self, level: i32, kind: i32, value: T) {
        use windows_sys::Win32::Networking::WinSock::CMSGHDR;
        let align = |size: usize| size.next_multiple_of(std::mem::size_of::<usize>());
        let header_size = align(std::mem::size_of::<CMSGHDR>());
        let length = header_size + std::mem::size_of::<T>();
        let space = header_size + align(std::mem::size_of::<T>());
        assert!(self.used + space <= self.data.len());
        // WSACMSGHDR and its payload use native-word alignment on Windows.
        unsafe {
            let base = self.data.as_mut_ptr().add(self.used);
            base.cast::<CMSGHDR>().write(CMSGHDR {
                cmsg_len: length,
                cmsg_level: level,
                cmsg_type: kind,
            });
            std::ptr::write_unaligned(base.add(header_size).cast::<T>(), value);
        }
        self.used += space;
    }

    #[cfg(unix)]
    fn interface(&mut self, peer: SocketAddr, interface: &EgressInterface) {
        if peer.is_ipv6() {
            // All-zero address leaves source selection to the specified NIC.
            let mut info: libc::in6_pktinfo = unsafe { std::mem::zeroed() };
            info.ipi6_ifindex = interface.index();
            self.push(libc::IPPROTO_IPV6, libc::IPV6_PKTINFO, info);
        } else {
            let mut info: libc::in_pktinfo = unsafe { std::mem::zeroed() };
            info.ipi_ifindex = interface.index() as _;
            self.push(libc::IPPROTO_IP, libc::IP_PKTINFO, info);
        }
        // Linux accepts SO_MARK ancillary data at the same per-message boundary
        // as IP_PKTINFO. Failure is propagated; no unmarked retry is allowed.
        #[cfg(target_os = "linux")]
        if let Some(mark) = interface.socket_mark() {
            self.push(libc::SOL_SOCKET, libc::SO_MARK, mark);
        }
    }

    #[cfg(windows)]
    fn interface(&mut self, peer: SocketAddr, interface: &EgressInterface) {
        use windows_sys::Win32::Networking::WinSock::*;
        if peer.is_ipv6() {
            self.push(
                IPPROTO_IPV6,
                IPV6_PKTINFO,
                IN6_PKTINFO {
                    ipi6_ifindex: interface.index(),
                    ..Default::default()
                },
            );
        } else {
            self.push(
                IPPROTO_IP,
                IP_PKTINFO,
                IN_PKTINFO {
                    ipi_ifindex: interface.index(),
                    ..Default::default()
                },
            );
        }
    }
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
#[path = "../../tests/packet_socket/ancillary.rs"]
mod tests;
