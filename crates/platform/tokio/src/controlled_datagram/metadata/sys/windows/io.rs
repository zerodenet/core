use std::{
    ffi::c_int,
    io::{self, IoSliceMut},
    mem,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    os::windows::io::AsRawSocket,
    ptr,
};

use socket2::SockRef;
use windows_sys::Win32::Networking::WinSock;

use super::{cmsg, CMSG_LEN};
use crate::controlled_datagram::metadata::{
    DatagramEcn, MetadataReceive, MetadataSendOutcome, MetadataTransmit,
};

pub(super) fn send_outcome(
    socket: SockRef<'_>,
    transmit: &MetadataTransmit<'_>,
) -> io::Result<MetadataSendOutcome> {
    if transmit.segment_size == Some(0)
        || transmit
            .segment_size
            .is_some_and(|size| size > u32::MAX as usize)
        || transmit.contents.len() > u32::MAX as usize
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid UDP native send length",
        ));
    }

    let mut ctrl_buf = cmsg::Aligned([0; CMSG_LEN]);
    let daddr = socket2::SockAddr::from(transmit.destination);

    let mut data = WinSock::WSABUF {
        buf: transmit.contents.as_ptr() as *mut _,
        len: transmit.contents.len() as _,
    };

    let ctrl = WinSock::WSABUF {
        buf: ctrl_buf.0.as_mut_ptr(),
        len: ctrl_buf.0.len() as _,
    };

    let mut wsa_msg = WinSock::WSAMSG {
        name: daddr.as_ptr() as *mut _,
        namelen: daddr.len(),
        lpBuffers: &mut data,
        Control: ctrl,
        dwBufferCount: 1,
        dwFlags: 0,
    };

    let mut encoder = unsafe { cmsg::Encoder::new(&mut wsa_msg) };

    if let Some(ip) = transmit.source_ip {
        let ip = socket2::SockAddr::from(SocketAddr::new(ip, 0));
        match ip.family() {
            WinSock::AF_INET => {
                let src_ip = unsafe { ptr::read(ip.as_ptr() as *const WinSock::SOCKADDR_IN) };
                let pktinfo = WinSock::IN_PKTINFO {
                    ipi_addr: src_ip.sin_addr,
                    ipi_ifindex: 0,
                };
                encoder.push(WinSock::IPPROTO_IP, WinSock::IP_PKTINFO, pktinfo);
            }
            WinSock::AF_INET6 => {
                let src_ip = unsafe { ptr::read(ip.as_ptr() as *const WinSock::SOCKADDR_IN6) };
                let pktinfo = WinSock::IN6_PKTINFO {
                    ipi6_addr: src_ip.sin6_addr,
                    ipi6_ifindex: unsafe { src_ip.Anonymous.sin6_scope_id },
                };
                encoder.push(WinSock::IPPROTO_IPV6, WinSock::IPV6_PKTINFO, pktinfo);
            }
            _ => return Err(io::Error::from(io::ErrorKind::InvalidInput)),
        }
    }

    let ecn = transmit.ecn.map_or(0, |x| x.as_bits() as c_int);
    let is_ipv4 = transmit.destination.is_ipv4()
        || matches!(transmit.destination.ip(), IpAddr::V6(addr) if addr.to_ipv4_mapped().is_some());
    if is_ipv4 {
        encoder.push(WinSock::IPPROTO_IP, WinSock::IP_ECN, ecn);
    } else {
        encoder.push(WinSock::IPPROTO_IPV6, WinSock::IPV6_ECN, ecn);
    }

    if let Some(segment_size) = transmit.segment_size {
        encoder.push(
            WinSock::IPPROTO_UDP,
            WinSock::UDP_SEND_MSG_SIZE,
            segment_size as u32,
        );
    }

    encoder.finish();

    let mut len = 0;
    let rc = unsafe {
        WinSock::WSASendMsg(
            socket.as_raw_socket() as usize,
            &wsa_msg,
            0,
            &mut len,
            ptr::null_mut(),
            None,
        )
    };
    if rc == 0 {
        Ok(MetadataSendOutcome::from_bytes(transmit, len as usize))
    } else {
        Err(io::Error::from_raw_os_error(unsafe {
            WinSock::WSAGetLastError()
        }))
    }
}

pub(super) fn recv_into(
    wsa_recvmsg_ptr: unsafe extern "system" fn(
        WinSock::SOCKET,
        *mut WinSock::WSAMSG,
        *mut u32,
        *mut windows_sys::Win32::System::IO::OVERLAPPED,
        WinSock::LPWSAOVERLAPPED_COMPLETION_ROUTINE,
    ) -> i32,
    socket: SockRef<'_>,
    bufs: &mut [IoSliceMut<'_>],
    meta: &mut [MetadataReceive],
) -> io::Result<usize> {
    let mut ctrl_buf = cmsg::Aligned([0; CMSG_LEN]);
    let mut source: WinSock::SOCKADDR_INET = unsafe { mem::zeroed() };
    let mut data = WinSock::WSABUF {
        buf: bufs[0].as_mut_ptr(),
        len: bufs[0].len() as _,
    };
    let ctrl = WinSock::WSABUF {
        buf: ctrl_buf.0.as_mut_ptr(),
        len: ctrl_buf.0.len() as _,
    };
    let mut wsa_msg = WinSock::WSAMSG {
        name: ptr::from_mut(&mut source).cast(),
        namelen: mem::size_of_val(&source) as _,
        lpBuffers: &mut data,
        Control: ctrl,
        dwBufferCount: 1,
        dwFlags: 0,
    };

    let mut len = 0;
    unsafe {
        let rc = (wsa_recvmsg_ptr)(
            socket.as_raw_socket() as usize,
            &mut wsa_msg,
            &mut len,
            ptr::null_mut(),
            None,
        );
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
    }

    let addr = unsafe {
        let (_, addr) = socket2::SockAddr::try_init(|addr_storage, sock_len| {
            *sock_len = mem::size_of_val(&source) as _;
            ptr::copy_nonoverlapping(&source, addr_storage.cast(), 1);
            Ok(())
        })?;
        addr.as_socket()
    };

    let mut ecn_bits = 0;
    let mut dst_ip = None;
    let mut stride = len;

    let cmsg_iter = unsafe { cmsg::Iter::new(&wsa_msg) };
    for cmsg in cmsg_iter {
        const UDP_COALESCED_INFO: i32 = WinSock::UDP_COALESCED_INFO as i32;
        match (cmsg.cmsg_level, cmsg.cmsg_type) {
            (WinSock::IPPROTO_IP, WinSock::IP_PKTINFO) => {
                let pktinfo =
                    unsafe { cmsg::decode::<WinSock::IN_PKTINFO, WinSock::CMSGHDR>(cmsg) };
                let ip4 = Ipv4Addr::from(u32::from_be(unsafe { pktinfo.ipi_addr.S_un.S_addr }));
                dst_ip = Some(ip4.into());
            }
            (WinSock::IPPROTO_IPV6, WinSock::IPV6_PKTINFO) => {
                let pktinfo =
                    unsafe { cmsg::decode::<WinSock::IN6_PKTINFO, WinSock::CMSGHDR>(cmsg) };
                dst_ip = Some(IpAddr::from(unsafe { pktinfo.ipi6_addr.u.Byte }));
            }
            (WinSock::IPPROTO_IP, WinSock::IP_ECN) | (WinSock::IPPROTO_IPV6, WinSock::IPV6_ECN) => {
                ecn_bits = unsafe { cmsg::decode::<c_int, WinSock::CMSGHDR>(cmsg) };
            }
            (WinSock::IPPROTO_UDP, UDP_COALESCED_INFO) => {
                stride = unsafe { cmsg::decode::<u32, WinSock::CMSGHDR>(cmsg) };
            }
            _ => {}
        }
    }

    meta[0] = MetadataReceive {
        source: addr.unwrap(),
        destination_ip: dst_ip,
        len: len as usize,
        stride: stride as usize,
        ecn: DatagramEcn::from_bits(ecn_bits as u8),
    };
    Ok(1)
}
