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
    use std::{os::windows::io::FromRawSocket, sync::OnceLock};
    use windows_sys::Win32::Networking::WinSock::{
        WSAGetLastError, WSASocketW, WSAStartup, AF_INET, AF_INET6, INVALID_SOCKET, IPPROTO_TCP,
        SOCK_STREAM, WSADATA, WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED,
    };
    // socket2's Windows initialization binds/drops a hidden dummy UDP socket.
    // Initialize Winsock directly instead. Keep this one process-wide startup
    // reference alive; per-listener cleanup must not tear down other sockets.
    static STARTUP: OnceLock<i32> = OnceLock::new();
    let code = *STARTUP.get_or_init(|| {
        let mut data = std::mem::MaybeUninit::<WSADATA>::uninit();
        // SAFETY: writable WSADATA, Winsock2.2, no native socket is created.
        unsafe { WSAStartup(0x0202, data.as_mut_ptr()) }
    });
    if code != 0 {
        return Err(io::Error::from_raw_os_error(code));
    }
    // SAFETY: exact TCP family/protocol; atomic no-inherit and overlapped flags.
    let original = unsafe {
        WSASocketW(
            if address.is_ipv4() {
                AF_INET as i32
            } else {
                AF_INET6 as i32
            },
            SOCK_STREAM,
            IPPROTO_TCP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if original == INVALID_SOCKET {
        // Preserve the actual failing syscall's error, before any other call.
        return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
    }
    // SAFETY: the successfully created original is uniquely transferred, with
    // no fallible work before installation in the already persistent owner.
    Ok(unsafe { socket2::Socket::from_raw_socket(original as _) })
}
