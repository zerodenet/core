mod io;

use std::{
    ffi::c_int,
    io::{self as std_io, IoSliceMut},
    mem,
    os::windows::io::AsRawSocket,
    ptr,
};

use socket2::SockRef;
use windows_sys::Win32::Networking::WinSock;

use super::{
    cmsg::{self, CMsgHdr},
    NativeProbes, ProbeKind,
};
use crate::controlled_datagram::metadata::{
    MetadataReceive, MetadataSendOutcome, MetadataTransmit,
};

pub(in super::super) const BATCH_SIZE: usize = 1;
pub(super) const CMSG_LEN: usize = 128;
const OPTION_ON: u32 = 1;

#[derive(Debug)]
pub(in super::super) struct UdpSocketState {
    recvmsg: WinSock::LPFN_WSARECVMSG,
    max_gso_segments: usize,
}

impl UdpSocketState {
    /// Query `WSARecvMsg` and configure ancillary socket options on the already-owned
    /// provider `SOCKET` without creating unowned dummy sockets or global caches.
    pub(in super::super) fn new_with_owned_probes(
        socket: SockRef<'_>,
        probes: &mut dyn NativeProbes,
    ) -> std_io::Result<Self> {
        assert!(
            CMSG_LEN
                >= WinSock::CMSGHDR::cmsg_space(mem::size_of::<WinSock::IN6_PKTINFO>())
                    + WinSock::CMSGHDR::cmsg_space(mem::size_of::<c_int>())
                    + WinSock::CMSGHDR::cmsg_space(mem::size_of::<u32>())
        );
        assert!(
            mem::align_of::<WinSock::CMSGHDR>() <= mem::align_of::<cmsg::Aligned<[u8; 0]>>(),
            "control message buffers will be misaligned"
        );

        socket.set_nonblocking(true)?;
        let addr = socket.local_addr()?;
        let is_ipv6 = addr.as_socket_ipv6().is_some();
        let v6only = if !is_ipv6 {
            false
        } else {
            unsafe {
                let mut result: u32 = 0;
                let mut len = mem::size_of_val(&result) as i32;
                let rc = WinSock::getsockopt(
                    socket.as_raw_socket() as _,
                    WinSock::IPPROTO_IPV6,
                    WinSock::IPV6_V6ONLY as _,
                    (&mut result as *mut u32).cast(),
                    &mut len,
                );
                if rc == -1 {
                    return Err(std_io::Error::last_os_error());
                }
                result != 0
            }
        };
        let is_ipv4 = addr.as_socket_ipv4().is_some() || !v6only;

        let recvmsg = query_owned_recvmsg(socket.as_raw_socket() as _)?;
        if recvmsg.is_none() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::Unsupported,
                "network stack does not support WSARecvMsg function",
            ));
        }

        if is_ipv4 {
            set_socket_option(
                &*socket,
                WinSock::IPPROTO_IP,
                WinSock::IP_DONTFRAGMENT,
                OPTION_ON,
            )?;
            set_socket_option(
                &*socket,
                WinSock::IPPROTO_IP,
                WinSock::IP_PKTINFO,
                OPTION_ON,
            )?;
            set_socket_option(
                &*socket,
                WinSock::IPPROTO_IP,
                WinSock::IP_RECVECN,
                OPTION_ON,
            )?;
        }

        if is_ipv6 {
            set_socket_option(
                &*socket,
                WinSock::IPPROTO_IPV6,
                WinSock::IPV6_DONTFRAG,
                OPTION_ON,
            )?;
            set_socket_option(
                &*socket,
                WinSock::IPPROTO_IPV6,
                WinSock::IPV6_PKTINFO,
                OPTION_ON,
            )?;
            set_socket_option(
                &*socket,
                WinSock::IPPROTO_IPV6,
                WinSock::IPV6_RECVECN,
                OPTION_ON,
            )?;
        }

        let max_gso_segments = probes.detect(ProbeKind::Gso)?;
        Ok(Self {
            recvmsg,
            max_gso_segments,
        })
    }

    pub(in super::super) fn try_send_outcome(
        &self,
        socket: SockRef<'_>,
        transmit: &MetadataTransmit<'_>,
    ) -> std_io::Result<MetadataSendOutcome> {
        io::send_outcome(socket, transmit)
    }

    pub(in super::super) fn recv(
        &self,
        socket: SockRef<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [MetadataReceive],
    ) -> std_io::Result<usize> {
        let wsa_recvmsg_ptr = self.recvmsg.expect("valid function pointer for WSARecvMsg");
        io::recv_into(wsa_recvmsg_ptr, socket, bufs, meta)
    }

    #[inline]
    pub(in super::super) fn max_gso_segments(&self) -> usize {
        self.max_gso_segments
    }

    #[inline]
    pub(in super::super) fn gro_segments(&self) -> usize {
        64
    }

    #[inline]
    pub(in super::super) fn may_fragment(&self) -> bool {
        false
    }
}

pub(super) fn capability_without_probe(kind: ProbeKind) -> Option<usize> {
    match kind {
        ProbeKind::Gso => None,
        ProbeKind::Gro => Some(64),
    }
}

pub(super) fn probe_owned_socket(kind: ProbeKind, socket: SockRef<'_>) -> std_io::Result<usize> {
    match kind {
        ProbeKind::Gso => set_socket_option(
            &*socket,
            WinSock::IPPROTO_UDP,
            WinSock::UDP_SEND_MSG_SIZE,
            1500,
        )
        .map(|()| 512),
        ProbeKind::Gro => Ok(64),
    }
}

fn set_socket_option(
    socket: &impl AsRawSocket,
    level: i32,
    name: i32,
    value: u32,
) -> std_io::Result<()> {
    let rc = unsafe {
        WinSock::setsockopt(
            socket.as_raw_socket() as usize,
            level,
            name,
            (&value as *const u32).cast(),
            mem::size_of_val(&value) as _,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std_io::Error::last_os_error())
    }
}

fn query_owned_recvmsg(socket: WinSock::SOCKET) -> std_io::Result<WinSock::LPFN_WSARECVMSG> {
    let guid = WinSock::WSAID_WSARECVMSG;
    let mut result = None;
    let mut len = 0;
    let rc = unsafe {
        WinSock::WSAIoctl(
            socket,
            WinSock::SIO_GET_EXTENSION_FUNCTION_POINTER,
            ptr::from_ref(&guid).cast(),
            mem::size_of_val(&guid) as u32,
            ptr::from_mut(&mut result).cast(),
            mem::size_of_val(&result) as u32,
            &mut len,
            ptr::null_mut(),
            None,
        )
    };
    if rc == -1 {
        return Err(std_io::Error::from_raw_os_error(unsafe {
            WinSock::WSAGetLastError()
        }));
    }
    if len as usize != mem::size_of::<WinSock::LPFN_WSARECVMSG>() || result.is_none() {
        return Err(std_io::Error::new(
            std_io::ErrorKind::Unsupported,
            "provider returned no valid WSARecvMsg function",
        ));
    }
    Ok(result)
}
