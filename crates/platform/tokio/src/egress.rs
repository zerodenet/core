use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio::net::TcpSocket;

/// Stable identity of the physical interface that must bypass an active TUN
/// default route. Both fields are retained because Linux binds by name while
/// macOS and Windows bind by interface index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressInterface {
    name: Arc<str>,
    index: u32,
}

impl EgressInterface {
    pub fn new(name: impl Into<Arc<str>>, index: u32) -> io::Result<Self> {
        let name = name.into();
        if name.is_empty() || index == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "egress interface requires a non-empty name and non-zero index",
            ));
        }
        Ok(Self { name, index })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> u32 {
        self.index
    }
}

/// Shared selector read by every TCP and UDP socket factory. Updating the
/// selector does not mutate existing sockets; it only affects new flows.
#[derive(Debug, Clone, Default)]
pub struct EgressInterfaceControl(Arc<RwLock<EgressInterfaces>>);

#[derive(Debug, Default)]
struct EgressInterfaces {
    ipv4: Option<EgressInterface>,
    ipv6: Option<EgressInterface>,
}

impl EgressInterfaceControl {
    pub fn current(&self) -> Option<EgressInterface> {
        let interfaces = self.0.read().expect("egress interface lock poisoned");
        interfaces.ipv4.clone().or_else(|| interfaces.ipv6.clone())
    }

    pub fn current_for(&self, ipv6: bool) -> Option<EgressInterface> {
        let interfaces = self.0.read().expect("egress interface lock poisoned");
        if ipv6 {
            interfaces.ipv6.clone()
        } else {
            interfaces.ipv4.clone()
        }
    }

    pub fn replace(&self, interface: Option<EgressInterface>) -> Option<EgressInterface> {
        let mut interfaces = self.0.write().expect("egress interface lock poisoned");
        let previous = interfaces.ipv4.clone().or_else(|| interfaces.ipv6.clone());
        interfaces.ipv4 = interface.clone();
        interfaces.ipv6 = interface;
        previous
    }

    pub fn replace_for(&self, ipv6: bool, interface: Option<EgressInterface>) {
        let mut interfaces = self.0.write().expect("egress interface lock poisoned");
        if ipv6 {
            interfaces.ipv6 = interface;
        } else {
            interfaces.ipv4 = interface;
        }
    }

    pub fn clear(&self) {
        *self.0.write().expect("egress interface lock poisoned") = EgressInterfaces::default();
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn bind_tcp_to_interface(
    socket: &TcpSocket,
    _peer: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    bind_fd_to_name(socket.as_raw_fd(), interface.name())
}

#[cfg(target_os = "linux")]
pub(crate) fn bind_udp_to_interface(
    socket: &std::net::UdpSocket,
    _local: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    bind_fd_to_name(socket.as_raw_fd(), interface.name())
}

#[cfg(target_os = "linux")]
fn bind_fd_to_name(fd: std::os::fd::RawFd, name: &str) -> io::Result<()> {
    let name = std::ffi::CString::new(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "interface name contains a nul byte",
        )
    })?;
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            name.as_ptr().cast(),
            name.as_bytes_with_nul().len() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn bind_tcp_to_interface(
    socket: &TcpSocket,
    peer: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    bind_fd_to_index(socket.as_raw_fd(), peer.is_ipv6(), interface.index())
}

#[cfg(target_os = "macos")]
pub(crate) fn bind_udp_to_interface(
    socket: &std::net::UdpSocket,
    local: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    bind_fd_to_index(socket.as_raw_fd(), local.is_ipv6(), interface.index())
}

#[cfg(target_os = "macos")]
fn bind_fd_to_index(fd: std::os::fd::RawFd, ipv6: bool, index: u32) -> io::Result<()> {
    let (level, option) = if ipv6 {
        (libc::IPPROTO_IPV6, libc::IPV6_BOUND_IF)
    } else {
        (libc::IPPROTO_IP, libc::IP_BOUND_IF)
    };
    let result = unsafe {
        libc::setsockopt(
            fd,
            level,
            option,
            (&index as *const u32).cast(),
            std::mem::size_of::<u32>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
pub(crate) fn bind_tcp_to_interface(
    socket: &TcpSocket,
    peer: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    bind_socket_to_index(socket.as_raw_socket(), peer.is_ipv6(), interface.index())
}

#[cfg(windows)]
pub(crate) fn bind_udp_to_interface(
    socket: &std::net::UdpSocket,
    local: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    bind_socket_to_index(socket.as_raw_socket(), local.is_ipv6(), interface.index())
}

#[cfg(windows)]
fn bind_socket_to_index(
    socket: std::os::windows::io::RawSocket,
    ipv6: bool,
    index: u32,
) -> io::Result<()> {
    use windows_sys::Win32::Networking::WinSock::{
        setsockopt, WSAGetLastError, IPPROTO_IP, IPPROTO_IPV6, IPV6_UNICAST_IF, IP_UNICAST_IF,
        SOCKET_ERROR,
    };
    let (level, option, value) = if ipv6 {
        (IPPROTO_IPV6, IPV6_UNICAST_IF, index)
    } else {
        (IPPROTO_IP, IP_UNICAST_IF, index.to_be())
    };
    let result = unsafe {
        setsockopt(
            socket as usize,
            level,
            option,
            (&value as *const u32).cast(),
            std::mem::size_of::<u32>() as i32,
        )
    };
    if result != SOCKET_ERROR {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn bind_tcp_to_interface(
    _socket: &TcpSocket,
    _peer: SocketAddr,
    _interface: &EgressInterface,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "egress interface binding is unsupported on this platform",
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn bind_udp_to_interface(
    _socket: &std::net::UdpSocket,
    _local: SocketAddr,
    _interface: &EgressInterface,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "egress interface binding is unsupported on this platform",
    ))
}
