//! Query the actual pending connection even when SIO_TCP_INFO rejects it.
use std::{io, net::SocketAddr};
use windows_sys::Win32::{
    Foundation::ERROR_INSUFFICIENT_BUFFER,
    NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCPROW_OWNER_PID, MIB_TCPTABLE_OWNER_PID, MIB_TCP_STATE_SYN_SENT,
        TCP_TABLE_OWNER_PID_ALL,
    },
    Networking::WinSock::AF_INET,
};

pub(super) fn assert_connect_waiting(local: SocketAddr, peer: SocketAddr) {
    let (SocketAddr::V4(local), SocketAddr::V4(peer)) = (local, peer) else {
        panic!("IPv4 backlog fixture required");
    };
    let mut size = 0;
    // SAFETY: the first documented size query writes only the supplied DWORD.
    // https://learn.microsoft.com/en-us/windows/win32/api/iphlpapi/nf-iphlpapi-getextendedtcptable
    let status = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            AF_INET.into(),
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    assert_eq!(status, ERROR_INSUFFICIENT_BUFFER);
    for _ in 0..4 {
        assert!(
            size > 0 && size <= 16 * 1024 * 1024,
            "bounded native TCP census"
        );
        // DWORD alignment also covers both returned C structures. Length, not
        // capacity, describes writable initialized storage for the system call.
        let mut words = vec![0_u32; (size as usize).div_ceil(4)];
        // SAFETY: the aligned buffer contains at least size writable bytes.
        let status = unsafe {
            GetExtendedTcpTable(
                words.as_mut_ptr().cast(),
                &mut size,
                0,
                AF_INET.into(),
                TCP_TABLE_OWNER_PID_ALL,
                0,
            )
        };
        if status == ERROR_INSUFFICIENT_BUFFER {
            continue;
        }
        assert_eq!(
            status,
            0,
            "native TCP table: {}",
            io::Error::from_raw_os_error(status as i32)
        );
        let offset = std::mem::offset_of!(MIB_TCPTABLE_OWNER_PID, table);
        let count = words[0] as usize;
        assert!(offset + count * std::mem::size_of::<MIB_TCPROW_OWNER_PID>() <= size as usize);
        for index in 0..count {
            // SAFETY: the successful call's byte length and entry count bound
            // this C row; read_unaligned does not assume additional alignment.
            let row = unsafe {
                words
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset + index * std::mem::size_of::<MIB_TCPROW_OWNER_PID>())
                    .cast::<MIB_TCPROW_OWNER_PID>()
                    .read_unaligned()
            };
            if row.dwOwningPid == std::process::id()
                && row.dwLocalAddr.to_ne_bytes() == local.ip().octets()
                && u16::from_be(row.dwLocalPort as u16) == local.port()
                && row.dwRemoteAddr.to_ne_bytes() == peer.ip().octets()
                && u16::from_be(row.dwRemotePort as u16) == peer.port()
            {
                assert_eq!(
                    row.dwState, MIB_TCP_STATE_SYN_SENT as u32,
                    "actual same-process four-tuple must still be SYN_SENT"
                );
                return;
            }
        }
        panic!("actual pending native TCP tuple absent: {local} -> {peer}");
    }
    panic!("native TCP census changed beyond the bounded query budget");
}
