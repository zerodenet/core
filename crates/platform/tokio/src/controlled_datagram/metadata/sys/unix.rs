mod msg;
mod offload;

use std::{
    io::{self, IoSliceMut},
    mem,
    os::unix::io::AsRawFd,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use socket2::SockRef;

use super::{cmsg, NativeProbes, ProbeKind};
use crate::controlled_datagram::metadata::{
    MetadataReceive, MetadataSendOutcome, MetadataTransmit,
};

#[cfg(any(target_os = "openbsd", target_os = "netbsd"))]
const IPV6_DONTFRAG: libc::c_int = 62;
#[cfg(not(any(target_os = "openbsd", target_os = "netbsd")))]
const IPV6_DONTFRAG: libc::c_int = libc::IPV6_DONTFRAG;

#[cfg(not(target_vendor = "apple"))]
pub(in super::super) const BATCH_SIZE: usize = 32;
#[cfg(target_vendor = "apple")]
pub(in super::super) const BATCH_SIZE: usize = 1;

pub(super) const CMSG_LEN: usize = 88;
pub(super) const OPTION_ON: libc::c_int = 1;

#[derive(Debug)]
pub(in super::super) struct UdpSocketState {
    max_gso_segments: AtomicUsize,
    gro_segments: usize,
    may_fragment: bool,
    sendmsg_einval: AtomicBool,
}

impl UdpSocketState {
    pub(in super::super) fn new_with_owned_probes(
        io: SockRef<'_>,
        probes: &mut dyn NativeProbes,
    ) -> io::Result<Self> {
        let mut cmsg_platform_space = 0;
        if cfg!(target_os = "linux")
            || cfg!(target_os = "freebsd")
            || cfg!(target_os = "openbsd")
            || cfg!(target_os = "netbsd")
            || cfg!(target_vendor = "apple")
            || cfg!(target_os = "android")
            || cfg!(target_os = "solaris")
            || cfg!(target_os = "illumos")
        {
            cmsg_platform_space +=
                unsafe { libc::CMSG_SPACE(mem::size_of::<libc::in6_pktinfo>() as _) as usize };
        }

        assert!(
            CMSG_LEN
                >= unsafe { libc::CMSG_SPACE(mem::size_of::<libc::c_int>() as _) as usize }
                    + cmsg_platform_space
        );
        assert!(
            mem::align_of::<libc::cmsghdr>() <= mem::align_of::<cmsg::Aligned<[u8; 0]>>(),
            "control message buffers will be misaligned"
        );

        io.set_nonblocking(true)?;

        let addr = io.local_addr()?;
        let is_ipv4 = addr.family() == libc::AF_INET as libc::sa_family_t;

        #[cfg(not(any(
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "solaris",
            target_os = "illumos"
        )))]
        if is_ipv4 || !io.only_v6()? {
            let _ = set_socket_option(&*io, libc::IPPROTO_IP, libc::IP_RECVTOS, OPTION_ON);
        }

        let mut may_fragment = false;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let _ = set_socket_option(&*io, libc::SOL_UDP, offload::UDP_GRO, OPTION_ON);
            may_fragment |= !set_socket_option_supported(
                &*io,
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                libc::IP_PMTUDISC_PROBE,
            )?;
            if is_ipv4 {
                set_socket_option(&*io, libc::IPPROTO_IP, libc::IP_PKTINFO, OPTION_ON)?;
            } else {
                may_fragment |= !set_socket_option_supported(
                    &*io,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_MTU_DISCOVER,
                    libc::IPV6_PMTUDISC_PROBE,
                )?;
            }
        }
        #[cfg(any(target_os = "freebsd", target_vendor = "apple"))]
        if is_ipv4 {
            may_fragment |=
                !set_socket_option_supported(&*io, libc::IPPROTO_IP, libc::IP_DONTFRAG, OPTION_ON)?;
        }
        #[cfg(any(
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd",
            target_vendor = "apple",
            target_os = "solaris",
            target_os = "illumos"
        ))]
        if is_ipv4 {
            set_socket_option(&*io, libc::IPPROTO_IP, libc::IP_RECVDSTADDR, OPTION_ON)?;
        }

        if !is_ipv4 {
            set_socket_option(&*io, libc::IPPROTO_IPV6, libc::IPV6_RECVPKTINFO, OPTION_ON)?;
            set_socket_option(&*io, libc::IPPROTO_IPV6, libc::IPV6_RECVTCLASS, OPTION_ON)?;
            may_fragment |=
                !set_socket_option_supported(&*io, libc::IPPROTO_IPV6, IPV6_DONTFRAG, OPTION_ON)?;
        }

        let max_gso_segments = probes.detect(ProbeKind::Gso)?;
        let gro_segments = probes.detect(ProbeKind::Gro)?;
        Ok(Self {
            max_gso_segments: AtomicUsize::new(max_gso_segments),
            gro_segments,
            may_fragment,
            sendmsg_einval: AtomicBool::new(false),
        })
    }

    pub(in super::super) fn try_send_outcome(
        &self,
        socket: SockRef<'_>,
        transmit: &MetadataTransmit<'_>,
    ) -> io::Result<MetadataSendOutcome> {
        if transmit
            .segment_size
            .is_some_and(|size| size == 0 || size > u16::MAX as usize)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid UDP segment size",
            ));
        }
        if self.sendmsg_einval() && transmit.ecn.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native ECN ancillary fallback is unavailable",
            ));
        }
        msg::send_outcome(self, socket, transmit)
    }

    pub(in super::super) fn recv(
        &self,
        socket: SockRef<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [MetadataReceive],
    ) -> io::Result<usize> {
        msg::recv_into(socket, bufs, meta)
    }

    #[inline]
    pub(in super::super) fn max_gso_segments(&self) -> usize {
        self.max_gso_segments.load(Ordering::Relaxed)
    }

    #[inline]
    pub(in super::super) fn gro_segments(&self) -> usize {
        self.gro_segments
    }

    #[inline]
    pub(in super::super) fn may_fragment(&self) -> bool {
        self.may_fragment
    }

    pub(super) fn sendmsg_einval(&self) -> bool {
        self.sendmsg_einval.load(Ordering::Relaxed)
    }

    #[cfg(not(any(target_vendor = "apple", target_os = "openbsd", target_os = "netbsd")))]
    pub(super) fn set_sendmsg_einval(&self) {
        self.sendmsg_einval.store(true, Ordering::Relaxed);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(super) fn disable_gso(&self) {
        self.max_gso_segments.store(1, Ordering::Relaxed);
    }
}

pub(super) fn capability_without_probe(kind: ProbeKind) -> Option<usize> {
    offload::capability_without_probe(kind)
}

pub(super) fn probe_owned_socket(kind: ProbeKind, socket: SockRef<'_>) -> io::Result<usize> {
    offload::probe_owned_socket(kind, socket)
}

fn set_socket_option_supported(
    socket: &impl AsRawFd,
    level: libc::c_int,
    name: libc::c_int,
    value: libc::c_int,
) -> io::Result<bool> {
    match set_socket_option(socket, level, name, value) {
        Ok(()) => Ok(true),
        Err(err)
            if matches!(
                err.raw_os_error(),
                Some(libc::ENOPROTOOPT) | Some(libc::EOPNOTSUPP)
            ) =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

pub(super) fn set_socket_option(
    socket: &impl AsRawFd,
    level: libc::c_int,
    name: libc::c_int,
    value: libc::c_int,
) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            name,
            (&value as *const libc::c_int).cast(),
            mem::size_of_val(&value) as _,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
