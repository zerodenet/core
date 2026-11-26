#[cfg(not(any(
    target_vendor = "apple",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "solaris",
    target_os = "illumos"
)))]
use std::ptr;
use std::{
    io::{self, IoSliceMut},
    mem::{self, MaybeUninit},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::unix::io::AsRawFd,
};

use socket2::SockRef;

use super::{cmsg, offload, UdpSocketState, BATCH_SIZE, CMSG_LEN};
use crate::controlled_datagram::metadata::{
    DatagramEcn, MetadataReceive, MetadataSendOutcome, MetadataTransmit,
};

#[cfg(target_os = "freebsd")]
type IpTosTy = libc::c_uchar;
#[cfg(not(any(target_os = "freebsd", target_os = "netbsd")))]
type IpTosTy = libc::c_int;

#[cfg(not(any(target_vendor = "apple", target_os = "openbsd", target_os = "netbsd")))]
pub(super) fn send_outcome(
    state: &UdpSocketState,
    io: SockRef<'_>,
    transmit: &MetadataTransmit<'_>,
) -> io::Result<MetadataSendOutcome> {
    #[allow(unused_mut)]
    let mut encode_src_ip = true;
    #[cfg(target_os = "freebsd")]
    {
        let addr = io.local_addr()?;
        if addr.family() == libc::AF_INET as libc::sa_family_t {
            if let Some(socket) = addr.as_socket_ipv4() {
                encode_src_ip = socket.ip() == &Ipv4Addr::UNSPECIFIED;
            }
        }
    }
    let mut msg_hdr: libc::msghdr = unsafe { mem::zeroed() };
    let mut iovec: libc::iovec = unsafe { mem::zeroed() };
    let mut cmsgs = cmsg::Aligned([0u8; CMSG_LEN]);
    let dst_addr = socket2::SockAddr::from(transmit.destination);
    prepare_msg(
        transmit,
        &dst_addr,
        &mut msg_hdr,
        &mut iovec,
        &mut cmsgs,
        encode_src_ip,
        state.sendmsg_einval(),
    );

    loop {
        let n = unsafe { libc::sendmsg(io.as_raw_fd(), &msg_hdr, 0) };
        if n >= 0 {
            return Ok(MetadataSendOutcome::from_bytes(transmit, n as usize));
        }
        let e = io::Error::last_os_error();
        match e.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Err(e),
            _ => {
                #[cfg(any(target_os = "linux", target_os = "android"))]
                if let Some(libc::EIO) | Some(libc::EINVAL) = e.raw_os_error() {
                    if state.max_gso_segments() > 1 {
                        state.disable_gso();
                    }
                }
                if e.raw_os_error() == Some(libc::EINVAL) && !state.sendmsg_einval() {
                    if transmit.ecn.is_some() {
                        return Err(e);
                    }
                    state.set_sendmsg_einval();
                    prepare_msg(
                        transmit,
                        &dst_addr,
                        &mut msg_hdr,
                        &mut iovec,
                        &mut cmsgs,
                        encode_src_ip,
                        state.sendmsg_einval(),
                    );
                    continue;
                }
                return Err(e);
            }
        }
    }
}

#[cfg(any(target_vendor = "apple", target_os = "openbsd", target_os = "netbsd"))]
pub(super) fn send_outcome(
    state: &UdpSocketState,
    io: SockRef<'_>,
    transmit: &MetadataTransmit<'_>,
) -> io::Result<MetadataSendOutcome> {
    let mut hdr: libc::msghdr = unsafe { mem::zeroed() };
    let mut iov: libc::iovec = unsafe { mem::zeroed() };
    let mut ctrl = cmsg::Aligned([0u8; CMSG_LEN]);
    let addr = socket2::SockAddr::from(transmit.destination);
    prepare_msg(
        transmit,
        &addr,
        &mut hdr,
        &mut iov,
        &mut ctrl,
        true,
        state.sendmsg_einval(),
    );
    loop {
        let n = unsafe { libc::sendmsg(io.as_raw_fd(), &hdr, 0) };
        if n >= 0 {
            return Ok(MetadataSendOutcome::from_bytes(transmit, n as usize));
        }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(e);
    }
}

#[cfg(not(any(
    target_vendor = "apple",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "solaris",
    target_os = "illumos"
)))]
pub(super) fn recv_into(
    io: SockRef<'_>,
    bufs: &mut [IoSliceMut<'_>],
    meta: &mut [MetadataReceive],
) -> io::Result<usize> {
    let mut names = [MaybeUninit::<libc::sockaddr_storage>::uninit(); BATCH_SIZE];
    let mut ctrls = [cmsg::Aligned(MaybeUninit::<[u8; CMSG_LEN]>::uninit()); BATCH_SIZE];
    let mut hdrs = unsafe { mem::zeroed::<[libc::mmsghdr; BATCH_SIZE]>() };
    let max_msg_count = bufs.len().min(BATCH_SIZE);
    for i in 0..max_msg_count {
        prepare_recv(
            &mut bufs[i],
            &mut names[i],
            &mut ctrls[i],
            &mut hdrs[i].msg_hdr,
        );
    }
    let msg_count = loop {
        let n = unsafe {
            libc::recvmmsg(
                io.as_raw_fd(),
                hdrs.as_mut_ptr(),
                max_msg_count as _,
                0,
                ptr::null_mut::<libc::timespec>(),
            )
        };
        if n >= 0 {
            break n as usize;
        }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(e);
    };
    for i in 0..msg_count {
        meta[i] = decode_recv(&names[i], &hdrs[i].msg_hdr, hdrs[i].msg_len as usize);
    }
    Ok(msg_count)
}

#[cfg(any(
    target_vendor = "apple",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "solaris",
    target_os = "illumos"
))]
pub(super) fn recv_into(
    io: SockRef<'_>,
    bufs: &mut [IoSliceMut<'_>],
    meta: &mut [MetadataReceive],
) -> io::Result<usize> {
    let mut name = MaybeUninit::<libc::sockaddr_storage>::uninit();
    let mut ctrl = cmsg::Aligned(MaybeUninit::<[u8; CMSG_LEN]>::uninit());
    let mut hdr = unsafe { mem::zeroed::<libc::msghdr>() };
    let n = loop {
        prepare_recv(&mut bufs[0], &mut name, &mut ctrl, &mut hdr);
        let n = unsafe { libc::recvmsg(io.as_raw_fd(), &mut hdr, 0) };
        if n >= 0 {
            if hdr.msg_flags & libc::MSG_TRUNC != 0 {
                continue;
            }
            break n as usize;
        }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(e);
    };
    meta[0] = decode_recv(&name, &hdr, n);
    Ok(1)
}

fn prepare_msg(
    transmit: &MetadataTransmit<'_>,
    dst_addr: &socket2::SockAddr,
    hdr: &mut libc::msghdr,
    iov: &mut libc::iovec,
    ctrl: &mut cmsg::Aligned<[u8; CMSG_LEN]>,
    #[allow(unused_variables)] encode_src_ip: bool,
    sendmsg_einval: bool,
) {
    iov.iov_base = transmit.contents.as_ptr() as *const _ as *mut _;
    iov.iov_len = transmit.contents.len();
    hdr.msg_name = dst_addr.as_ptr() as *mut libc::c_void;
    hdr.msg_namelen = dst_addr.len();
    hdr.msg_iov = iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = ctrl.0.as_mut_ptr().cast();
    hdr.msg_controllen = CMSG_LEN as _;

    let mut encoder = unsafe { cmsg::Encoder::new(hdr) };
    let ecn = transmit.ecn.map_or(0, |x| x.as_bits() as libc::c_int);
    let is_ipv4 = transmit.destination.is_ipv4()
        || matches!(transmit.destination.ip(), IpAddr::V6(addr) if addr.to_ipv4_mapped().is_some());
    if is_ipv4 {
        if !sendmsg_einval {
            #[cfg(not(target_os = "netbsd"))]
            encoder.push(libc::IPPROTO_IP, libc::IP_TOS, ecn as IpTosTy);
        }
    } else {
        encoder.push(libc::IPPROTO_IPV6, libc::IPV6_TCLASS, ecn);
    }

    if let Some(segment_size) = transmit
        .segment_size
        .filter(|segment_size| *segment_size < transmit.contents.len())
    {
        offload::set_segment_size(&mut encoder, segment_size as u16);
    }

    if let Some(ip) = &transmit.source_ip {
        match ip {
            IpAddr::V4(v4) => {
                #[cfg(any(target_os = "linux", target_os = "android"))]
                {
                    let pktinfo = libc::in_pktinfo {
                        ipi_ifindex: 0,
                        ipi_spec_dst: libc::in_addr {
                            s_addr: u32::from_ne_bytes(v4.octets()),
                        },
                        ipi_addr: libc::in_addr { s_addr: 0 },
                    };
                    encoder.push(libc::IPPROTO_IP, libc::IP_PKTINFO, pktinfo);
                }
                #[cfg(any(
                    target_os = "freebsd",
                    target_os = "openbsd",
                    target_os = "netbsd",
                    target_vendor = "apple",
                    target_os = "solaris",
                    target_os = "illumos"
                ))]
                if encode_src_ip {
                    let addr = libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.octets()),
                    };
                    encoder.push(libc::IPPROTO_IP, libc::IP_RECVDSTADDR, addr);
                }
            }
            IpAddr::V6(v6) => {
                let pktinfo = libc::in6_pktinfo {
                    ipi6_ifindex: 0,
                    ipi6_addr: libc::in6_addr {
                        s6_addr: v6.octets(),
                    },
                };
                encoder.push(libc::IPPROTO_IPV6, libc::IPV6_PKTINFO, pktinfo);
            }
        }
    }
    encoder.finish();
}

fn prepare_recv(
    buf: &mut IoSliceMut<'_>,
    name: &mut MaybeUninit<libc::sockaddr_storage>,
    ctrl: &mut cmsg::Aligned<MaybeUninit<[u8; CMSG_LEN]>>,
    hdr: &mut libc::msghdr,
) {
    hdr.msg_name = name.as_mut_ptr().cast();
    hdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as _;
    hdr.msg_iov = buf as *mut IoSliceMut<'_> as *mut libc::iovec;
    hdr.msg_iovlen = 1;
    hdr.msg_control = ctrl.0.as_mut_ptr().cast();
    hdr.msg_controllen = CMSG_LEN as _;
    hdr.msg_flags = 0;
}

fn decode_recv(
    name: &MaybeUninit<libc::sockaddr_storage>,
    hdr: &libc::msghdr,
    len: usize,
) -> MetadataReceive {
    let name = unsafe { name.assume_init() };
    let mut ecn_bits = 0;
    let mut dst_ip = None;
    #[allow(unused_mut)]
    let mut stride = len;

    let cmsg_iter = unsafe { cmsg::Iter::new(hdr) };
    for cmsg in cmsg_iter {
        match (cmsg.cmsg_level, cmsg.cmsg_type) {
            (libc::IPPROTO_IP, libc::IP_TOS) => unsafe {
                ecn_bits = cmsg::decode::<u8, libc::cmsghdr>(cmsg);
            },
            #[cfg(not(any(
                target_os = "openbsd",
                target_os = "netbsd",
                target_os = "solaris",
                target_os = "illumos"
            )))]
            (libc::IPPROTO_IP, libc::IP_RECVTOS) => unsafe {
                ecn_bits = cmsg::decode::<u8, libc::cmsghdr>(cmsg);
            },
            (libc::IPPROTO_IPV6, libc::IPV6_TCLASS) => unsafe {
                #[allow(clippy::unnecessary_cast)]
                if cfg!(target_vendor = "apple")
                    && cmsg.cmsg_len as usize == libc::CMSG_LEN(mem::size_of::<u8>() as _) as usize
                {
                    ecn_bits = cmsg::decode::<u8, libc::cmsghdr>(cmsg);
                } else {
                    ecn_bits = cmsg::decode::<libc::c_int, libc::cmsghdr>(cmsg) as u8;
                }
            },
            #[cfg(any(target_os = "linux", target_os = "android"))]
            (libc::IPPROTO_IP, libc::IP_PKTINFO) => {
                let pktinfo = unsafe { cmsg::decode::<libc::in_pktinfo, libc::cmsghdr>(cmsg) };
                dst_ip = Some(IpAddr::V4(Ipv4Addr::from(
                    pktinfo.ipi_addr.s_addr.to_ne_bytes(),
                )));
            }
            #[cfg(any(
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
                target_vendor = "apple"
            ))]
            (libc::IPPROTO_IP, libc::IP_RECVDSTADDR) => {
                let in_addr = unsafe { cmsg::decode::<libc::in_addr, libc::cmsghdr>(cmsg) };
                dst_ip = Some(IpAddr::V4(Ipv4Addr::from(in_addr.s_addr.to_ne_bytes())));
            }
            (libc::IPPROTO_IPV6, libc::IPV6_PKTINFO) => {
                let pktinfo = unsafe { cmsg::decode::<libc::in6_pktinfo, libc::cmsghdr>(cmsg) };
                dst_ip = Some(IpAddr::V6(Ipv6Addr::from(pktinfo.ipi6_addr.s6_addr)));
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            (libc::SOL_UDP, offload::UDP_GRO) => unsafe {
                stride = cmsg::decode::<libc::c_int, libc::cmsghdr>(cmsg) as usize;
            },
            _ => {}
        }
    }

    let addr = match libc::c_int::from(name.ss_family) {
        libc::AF_INET => {
            let addr: &libc::sockaddr_in = unsafe { &*(&name as *const _ as *const _) };
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(addr.sin_port),
            ))
        }
        libc::AF_INET6 => {
            let addr: &libc::sockaddr_in6 = unsafe { &*(&name as *const _ as *const _) };
            SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(addr.sin6_addr.s6_addr),
                u16::from_be(addr.sin6_port),
                addr.sin6_flowinfo,
                addr.sin6_scope_id,
            ))
        }
        _ => unreachable!(),
    };

    MetadataReceive {
        source: addr,
        destination_ip: dst_ip,
        len,
        stride,
        ecn: DatagramEcn::from_bits(ecn_bits),
    }
}
