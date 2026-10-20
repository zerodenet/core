use super::*;
use std::net::{IpAddr, SocketAddr};
impl ControlledIcmpOwner {
    /// RAW create PermissionDenied alone permits one ICMP datagram fallback.
    /// Every post-create error instead explicitly retires that same original.
    pub fn bind(
        &mut self,
        address: IpAddr,
        egress: Option<&EgressInterface>,
    ) -> Result<ControlledIcmpSocket, IcmpAcquisitionError> {
        let entry = self.reserve(address.is_ipv6())?;
        self.bind_entry(entry, address, egress, || {})
    }
    pub(super) fn bind_entry(
        &mut self,
        entry: Arc<Entry>,
        address: IpAddr,
        egress: Option<&EgressInterface>,
        after_create: impl FnOnce(),
    ) -> Result<ControlledIcmpSocket, IcmpAcquisitionError> {
        // A create unwind has no proven disposition. Only an actual returned
        // error may turn this sentinel into KnownNoNative.
        *entry.native() = Native::Unknown(entry.uncertain.clone());
        let raw = create(address.is_ipv4(), false);
        let (original, mode) = match raw {
            Ok(socket) => (socket, IcmpMode::Raw),
            Err(source) if source.kind() == std_io::ErrorKind::PermissionDenied => {
                // No native handle returned: record this real failed attempt
                // before the sole fallback. Do not turn it into close evidence.
                *entry.native() = Native::NoNative;
                let source = Arc::new(source);
                self.first_raw_permission_source
                    .get_or_insert_with(|| source.clone());
                entry.metadata().raw_permission_source = Some(source);
                *entry.native() = Native::Unknown(entry.uncertain.clone());
                match create(address.is_ipv4(), true) {
                    Ok(socket) => (socket, IcmpMode::PingDatagram),
                    Err(source) => return Err(self.no_native(&entry, source)),
                }
            }
            Err(source) => return Err(self.no_native(&entry, source)),
        };
        *entry.native() = Native::Raw(original.into());
        entry.metadata().mode = Some(mode);
        after_create();
        let configured = {
            let native = entry.native();
            let Native::Raw(original) = &*native else {
                unreachable!()
            };
            let socket = socket2::SockRef::from(original);
            (|| {
                #[cfg(unix)]
                socket.set_cloexec(true)?;
                #[cfg(target_vendor = "apple")]
                socket.set_nosigpipe(true)?;
                socket.bind(&SocketAddr::new(address, 0).into())?;
                original.set_nonblocking(true)?;
                if let Some(interface) = egress {
                    crate::egress::bind_udp_to_interface(
                        original,
                        SocketAddr::new(address, 0),
                        interface,
                    )?;
                }
                Ok::<(), std_io::Error>(())
            })()
        };
        if let Err(source) = configured {
            return Err(self.failed(&entry, source));
        }
        {
            let mut native = entry.native();
            let Native::Raw(original) =
                std::mem::replace(&mut *native, Native::Unknown(entry.uncertain.clone()))
            else {
                unreachable!()
            };
            match tokio::net::UdpSocket::from_std(original) {
                Ok(socket) => *native = Native::Live(socket),
                Err(source) => {
                    let source = Arc::new(source);
                    self.first_acquisition_failure
                        .get_or_insert_with(|| source.clone());
                    entry.metadata().acquisition_source = Some(source.clone());
                    *native = Native::Unknown(source.clone());
                    return Err(IcmpAcquisitionError {
                        raw_permission_source: entry.metadata().raw_permission_source.clone(),
                        reservation: Some(entry.id),
                        outcome: IcmpAcquisitionOutcome::Unknown(entry.id),
                        source,
                    });
                }
            }
        }
        Ok(ControlledIcmpSocket { entry })
    }
    fn no_native(&mut self, entry: &Arc<Entry>, source: std_io::Error) -> IcmpAcquisitionError {
        let source = Arc::new(source);
        self.first_acquisition_failure
            .get_or_insert_with(|| source.clone());
        entry.metadata().acquisition_source = Some(source.clone());
        *entry.native() = Native::NoNative;
        self.reap();
        IcmpAcquisitionError {
            raw_permission_source: entry.metadata().raw_permission_source.clone(),
            reservation: Some(entry.id),
            outcome: IcmpAcquisitionOutcome::KnownNoNative,
            source,
        }
    }
    fn failed(&mut self, entry: &Arc<Entry>, source: std_io::Error) -> IcmpAcquisitionError {
        let source = Arc::new(source);
        self.first_acquisition_failure
            .get_or_insert_with(|| source.clone());
        entry.metadata().acquisition_source = Some(source.clone());
        let outcome = match entry.close() {
            Ok(receipt) => IcmpAcquisitionOutcome::Closed(receipt),
            Err(_) => IcmpAcquisitionOutcome::Unknown(entry.id),
        };
        if let Some(close) = entry.metadata().close_source.clone() {
            self.first_close_failure.get_or_insert(close);
        }
        self.reap();
        IcmpAcquisitionError {
            raw_permission_source: entry.metadata().raw_permission_source.clone(),
            reservation: Some(entry.id),
            outcome,
            source,
        }
    }
}
fn create(ipv4: bool, ping: bool) -> std_io::Result<socket2::Socket> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Networking::WinSock::{
            IPPROTO_ICMP, IPPROTO_ICMPV6, SOCK_DGRAM, SOCK_RAW,
        };
        crate::windows_socket::new(
            ipv4,
            if ping { SOCK_DGRAM } else { SOCK_RAW },
            if ipv4 { IPPROTO_ICMP } else { IPPROTO_ICMPV6 },
        )
    }
    #[cfg(not(windows))]
    {
        let kind = if ping {
            socket2::Type::DGRAM
        } else {
            socket2::Type::RAW
        };
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
            target_os = "cygwin"
        ))]
        let kind = kind.cloexec();
        socket2::Socket::new_raw(
            if ipv4 {
                socket2::Domain::IPV4
            } else {
                socket2::Domain::IPV6
            },
            kind,
            Some(if ipv4 {
                socket2::Protocol::ICMPV4
            } else {
                socket2::Protocol::ICMPV6
            }),
        )
    }
}
