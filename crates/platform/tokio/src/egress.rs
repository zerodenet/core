use std::io;
use std::net::{IpAddr, SocketAddr};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressRouteLookupStatus {
    Skipped,
    Resolved,
    Failed,
}

impl EgressRouteLookupStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skipped => "skipped",
            Self::Resolved => "resolved",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressBindingReason {
    Loopback,
    NoConfiguredInterface,
    TunEgressUnavailable,
    SystemRoute,
    TunRoute,
    TunAddressesUnavailable,
    RouteLookupFailed,
}

impl EgressBindingReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Loopback => "loopback",
            Self::NoConfiguredInterface => "no_configured_interface",
            Self::TunEgressUnavailable => "tun_egress_unavailable",
            Self::SystemRoute => "system_route",
            Self::TunRoute => "tun_route",
            Self::TunAddressesUnavailable => "tun_addresses_unavailable",
            Self::RouteLookupFailed => "route_lookup_failed",
        }
    }
}

/// Route probe and binding decision captured before an outbound socket is
/// opened. Keeping the decision intact lets upper layers attach the same facts
/// to successful and failed flow records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressSelection {
    interface: Option<EgressInterface>,
    route_source: Option<IpAddr>,
    route_lookup_status: EgressRouteLookupStatus,
    route_lookup_error: Option<String>,
    binding_reason: EgressBindingReason,
}

impl EgressSelection {
    pub fn interface(&self) -> Option<&EgressInterface> {
        self.interface.as_ref()
    }

    pub fn route_source(&self) -> Option<IpAddr> {
        self.route_source
    }

    pub fn route_lookup_status(&self) -> EgressRouteLookupStatus {
        self.route_lookup_status
    }

    pub fn route_lookup_error(&self) -> Option<&str> {
        self.route_lookup_error.as_deref()
    }

    pub fn binding_reason(&self) -> EgressBindingReason {
        self.binding_reason
    }

    pub fn ensure_connectable(&self) -> io::Result<()> {
        if self.binding_reason == EgressBindingReason::TunEgressUnavailable {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TUN capture route is active but no physical egress interface is available",
            ));
        }
        Ok(())
    }
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
    tunnel_addresses: Vec<IpAddr>,
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

    /// Select a forced physical egress only when the system route for `peer`
    /// would otherwise send the socket back through the active TUN device.
    ///
    /// More-specific routes through a LAN, VPN, or another local interface
    /// keep normal kernel routing. Failure to probe is fail-safe: the physical
    /// egress remains selected so an unknown route cannot create a TUN loop.
    pub fn current_for_peer(&self, peer: SocketAddr) -> Option<EgressInterface> {
        self.select_for_peer(peer).interface
    }

    pub fn try_current_for_peer(&self, peer: SocketAddr) -> io::Result<Option<EgressInterface>> {
        let selection = self.select_for_peer(peer);
        selection.ensure_connectable()?;
        Ok(selection.interface)
    }

    pub fn select_for_peer(&self, peer: SocketAddr) -> EgressSelection {
        if peer.ip().is_loopback() {
            return EgressSelection {
                interface: None,
                route_source: None,
                route_lookup_status: EgressRouteLookupStatus::Skipped,
                route_lookup_error: None,
                binding_reason: EgressBindingReason::Loopback,
            };
        }
        let (physical, tunnel_addresses) = {
            let interfaces = self.0.read().expect("egress interface lock poisoned");
            let physical = if peer.is_ipv6() {
                interfaces.ipv6.clone()
            } else {
                interfaces.ipv4.clone()
            };
            (physical, interfaces.tunnel_addresses.clone())
        };
        // A wildcard address is used as a family marker when creating a
        // reusable UDP socket; it is not a routable peer. Probing it makes
        // Windows and Linux report loopback as the route source, which can be
        // mistaken for an active TUN capture route. Preserve fail-closed
        // behavior once TUN addresses have actually been published, while a
        // normal runtime with no TUN state remains connectable.
        if peer.ip().is_unspecified() {
            let (interface, binding_reason) = match (physical, tunnel_addresses.is_empty()) {
                (None, true) => (None, EgressBindingReason::NoConfiguredInterface),
                (None, false) => (None, EgressBindingReason::TunEgressUnavailable),
                (Some(physical), true) => {
                    (Some(physical), EgressBindingReason::TunAddressesUnavailable)
                }
                (Some(physical), false) => (Some(physical), EgressBindingReason::TunRoute),
            };
            return EgressSelection {
                interface,
                route_source: None,
                route_lookup_status: EgressRouteLookupStatus::Skipped,
                route_lookup_error: None,
                binding_reason,
            };
        }
        let no_physical_interface = physical.is_none();
        if no_physical_interface && tunnel_addresses.is_empty() {
            let (route_source, route_lookup_status, route_lookup_error) =
                match route_source_for(peer) {
                    Ok(source) => (Some(source), EgressRouteLookupStatus::Resolved, None),
                    Err(error) => (
                        None,
                        EgressRouteLookupStatus::Failed,
                        Some(error.to_string()),
                    ),
                };
            let captured = route_source.is_some_and(|source| source.is_loopback());
            return EgressSelection {
                interface: None,
                route_source,
                route_lookup_status,
                route_lookup_error,
                binding_reason: if captured {
                    EgressBindingReason::TunEgressUnavailable
                } else {
                    EgressBindingReason::NoConfiguredInterface
                },
            };
        }
        if tunnel_addresses.is_empty() {
            return EgressSelection {
                interface: Some(physical.expect("physical egress checked above")),
                route_source: None,
                route_lookup_status: EgressRouteLookupStatus::Skipped,
                route_lookup_error: None,
                binding_reason: EgressBindingReason::TunAddressesUnavailable,
            };
        }
        let (route_source, route_lookup_status, route_lookup_error) = match route_source_for(peer) {
            Ok(source) => (Some(source), EgressRouteLookupStatus::Resolved, None),
            Err(error) => (
                None,
                EgressRouteLookupStatus::Failed,
                Some(error.to_string()),
            ),
        };
        let route_is_captured = route_source
            .is_some_and(|source| source.is_loopback() || tunnel_addresses.contains(&source));
        let (interface, binding_reason) = match (physical, route_is_captured, route_source) {
            (Some(physical), true, _) => (Some(physical), EgressBindingReason::TunRoute),
            (Some(_), false, Some(_)) => (None, EgressBindingReason::SystemRoute),
            (Some(physical), false, None) => {
                (Some(physical), EgressBindingReason::RouteLookupFailed)
            }
            (None, _, _) => (None, EgressBindingReason::TunEgressUnavailable),
        };
        EgressSelection {
            interface,
            route_source,
            route_lookup_status,
            route_lookup_error,
            binding_reason,
        }
    }

    pub fn replace_tunnel_addresses(&self, addresses: impl IntoIterator<Item = IpAddr>) {
        self.0
            .write()
            .expect("egress interface lock poisoned")
            .tunnel_addresses = addresses.into_iter().collect();
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

fn route_source_for(peer: SocketAddr) -> io::Result<IpAddr> {
    let wildcard = if peer.is_ipv6() {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    };
    let socket = std::net::UdpSocket::bind(wildcard)?;
    socket.connect(peer)?;
    socket.local_addr().map(|address| address.ip())
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

    // IP_UNICAST_IF alone leaves source-address selection to the stack. With
    // split-default Wintun routes active, Windows can then select a loopback
    // or tunnel source even though the outgoing interface is physical, and
    // connect(2) fails with WSAEHOSTUNREACH for otherwise reachable peers.
    // Resolve and bind the source owned by the selected physical interface
    // before constraining the unicast interface.
    socket.bind(windows_source_address(peer, interface.index())?)?;
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

#[cfg(windows)]
pub(crate) fn datagram_bind_address(
    peer: SocketAddr,
    interface: Option<&EgressInterface>,
) -> io::Result<SocketAddr> {
    match interface.filter(|_| !peer.ip().is_loopback()) {
        Some(interface) => windows_source_address(peer, interface.index()),
        None => Ok(wildcard_address(peer)),
    }
}

#[cfg(windows)]
fn windows_source_address(peer: SocketAddr, interface_index: u32) -> io::Result<SocketAddr> {
    use windows_sys::Win32::NetworkManagement::IpHelper::{GetBestRoute2, MIB_IPFORWARD_ROW2};
    use windows_sys::Win32::Networking::WinSock::{
        AF_INET, AF_INET6, IN6_ADDR, IN6_ADDR_0, IN_ADDR, IN_ADDR_0, SOCKADDR_IN, SOCKADDR_IN6,
        SOCKADDR_IN6_0, SOCKADDR_INET,
    };

    let destination = match peer {
        SocketAddr::V4(address) => SOCKADDR_INET {
            Ipv4: SOCKADDR_IN {
                sin_family: AF_INET,
                sin_port: 0,
                sin_addr: IN_ADDR {
                    S_un: IN_ADDR_0 {
                        S_addr: u32::from_ne_bytes(address.ip().octets()),
                    },
                },
                sin_zero: [0; 8],
            },
        },
        SocketAddr::V6(address) => SOCKADDR_INET {
            Ipv6: SOCKADDR_IN6 {
                sin6_family: AF_INET6,
                sin6_port: 0,
                sin6_flowinfo: address.flowinfo(),
                sin6_addr: IN6_ADDR {
                    u: IN6_ADDR_0 {
                        Byte: address.ip().octets(),
                    },
                },
                Anonymous: SOCKADDR_IN6_0 {
                    sin6_scope_id: address.scope_id(),
                },
            },
        },
    };
    let mut route = MIB_IPFORWARD_ROW2::default();
    let mut source = unsafe { std::mem::zeroed::<SOCKADDR_INET>() };
    let status = unsafe {
        GetBestRoute2(
            std::ptr::null(),
            interface_index,
            std::ptr::null(),
            &destination,
            0,
            &mut route,
            &mut source,
        )
    };
    if status != 0 {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!(
                "resolve source address for Windows interface {interface_index}: {}",
                io::Error::from_raw_os_error(status as i32)
            ),
        ));
    }

    match unsafe { source.si_family } {
        AF_INET => Ok(SocketAddr::new(
            IpAddr::V4(std::net::Ipv4Addr::from(unsafe {
                source.Ipv4.sin_addr.S_un.S_addr.to_ne_bytes()
            })),
            0,
        )),
        AF_INET6 => {
            let source = unsafe { source.Ipv6 };
            Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::from(unsafe { source.sin6_addr.u.Byte }),
                0,
                source.sin6_flowinfo,
                unsafe { source.Anonymous.sin6_scope_id },
            )))
        }
        family => Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("Windows route query returned address family {family}"),
        )),
    }
}

#[cfg(not(windows))]
pub(crate) fn datagram_bind_address(
    peer: SocketAddr,
    _interface: Option<&EgressInterface>,
) -> io::Result<SocketAddr> {
    Ok(wildcard_address(peer))
}

fn wildcard_address(peer: SocketAddr) -> SocketAddr {
    if peer.is_ipv4() {
        SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0)
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
