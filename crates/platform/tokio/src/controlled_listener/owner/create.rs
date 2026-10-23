use std::{io, net::SocketAddr};

#[cfg(not(windows))]
pub(super) fn socket(address: SocketAddr) -> io::Result<socket2::Socket> {
    let domain = if address.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let ty = socket2::Type::STREAM;
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "hurd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "cygwin",
    ))]
    let ty = ty.cloexec();
    // On Unix, socket2 new_raw is just socket(2), with no post-create flags
    // or implicit-Drop error window. Flags are set after persistent installation.
    socket2::Socket::new_raw(domain, ty, Some(socket2::Protocol::TCP))
}

#[cfg(windows)]
pub(super) fn socket(address: SocketAddr) -> io::Result<socket2::Socket> {
    use windows_sys::Win32::Networking::WinSock::{IPPROTO_TCP, SOCK_STREAM};
    crate::windows_socket::new(address.is_ipv4(), SOCK_STREAM, IPPROTO_TCP)
}
