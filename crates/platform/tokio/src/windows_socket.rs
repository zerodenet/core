//! Direct native creation for preinstalled TCP/UDP owners. No dummy socket.
use std::{io, os::windows::io::FromRawSocket, sync::OnceLock};
use windows_sys::Win32::Networking::WinSock::{
    WSAGetLastError, WSASocketW, WSAStartup, AF_INET, AF_INET6, INVALID_SOCKET, WSADATA,
    WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED,
};

pub(crate) fn new(ipv4: bool, kind: i32, protocol: i32) -> io::Result<socket2::Socket> {
    // socket2's Windows initialization binds/drops a hidden dummy UDP socket.
    // Keep one explicit process-wide Winsock2.2 reference instead. A resource's
    // cleanup must not tear down the Winsock reference used by other owners.
    static STARTUP: OnceLock<i32> = OnceLock::new();
    let code = *STARTUP.get_or_init(|| {
        let mut data = std::mem::MaybeUninit::<WSADATA>::uninit();
        // SAFETY: writable WSADATA; startup creates no native socket.
        unsafe { WSAStartup(0x0202, data.as_mut_ptr()) }
    });
    if code != 0 {
        return Err(io::Error::from_raw_os_error(code));
    }
    // SAFETY: callers supply the original native type/protocol, with atomic
    // no-inherit and overlapped flags for both actual TCP and UDP originals.
    let original = unsafe {
        WSASocketW(
            if ipv4 {
                AF_INET as i32
            } else {
                AF_INET6 as i32
            },
            kind,
            protocol,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if original == INVALID_SOCKET {
        // Capture the failing native call's source before any other operation.
        return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
    }
    // SAFETY: transfer the same uniquely owned SOCKET. No fallible work occurs
    // between creation and the caller's persistent-owner installation.
    Ok(unsafe { socket2::Socket::from_raw_socket(original as _) })
}
