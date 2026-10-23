use super::*;
use crate::{dial, EgressSelection};
use std::net::SocketAddr;
use zero_traits::{AddressFamily, DialPolicy};

#[derive(Debug)]
struct PhaseError {
    stage: &'static str,
    source: std_io::Error,
}
impl fmt::Display for PhaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.stage, self.source)
    }
}
impl std::error::Error for PhaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
fn phase(stage: &'static str, source: std_io::Error) -> Arc<std_io::Error> {
    Arc::new(std_io::Error::new(
        source.kind(),
        PhaseError { stage, source },
    ))
}
impl ControlledDatagramOwner {
    /// Pure policy preparation precedes socket creation. Once Socket::new
    /// succeeds, the original immediately enters this persistent owner.
    pub fn acquire(
        &mut self,
        peer: SocketAddr,
        preferred_port: Option<u16>,
        policy: &DialPolicy,
        selection: &EgressSelection,
    ) -> Result<ControlledDatagramSocket, DatagramAcquisitionError> {
        let entry = self.reserve()?;
        let prepared = (|| {
            let dial = dial::prepare(peer, policy, selection)?;
            let mut source = match dial.source {
                Some(source) => source,
                None => crate::egress::datagram_bind_address(dial.peer, dial.interface.as_ref())?,
            };
            source.set_port(preferred_port.unwrap_or(0));
            Ok::<_, std_io::Error>((
                source,
                dial.interface,
                policy
                    .effective_family()
                    .is_ok_and(|family| family == AddressFamily::OnlyIpv6),
            ))
        })();
        let (source, interface, v6_only) = match prepared {
            Ok(prepared) => prepared,
            Err(source) => return Err(self.no_native(&entry, phase("prepare", source))),
        };
        let socket = self.acquire_native(entry, source, preferred_port, v6_only, interface)?;
        self.register_native(socket)
    }
    /// Bind an exact server address without outbound policy or port fallback.
    pub(super) fn bind_native_listener(
        &mut self,
        source: SocketAddr,
    ) -> Result<ControlledDatagramSocket, DatagramAcquisitionError> {
        let socket = self.bind_raw_listener(source)?;
        self.register_native(socket)
    }
    pub(super) fn bind_raw_listener(
        &mut self,
        source: SocketAddr,
    ) -> Result<ControlledDatagramSocket, DatagramAcquisitionError> {
        let entry = self.reserve()?;
        self.acquire_native(entry, source, None, false, None)
    }
    fn acquire_native(
        &mut self,
        entry: Arc<Entry>,
        source: SocketAddr,
        preferred_port: Option<u16>,
        v6_only: bool,
        interface: Option<EgressInterface>,
    ) -> Result<ControlledDatagramSocket, DatagramAcquisitionError> {
        #[cfg(not(windows))]
        let original = {
            let domain = if source.is_ipv4() {
                socket2::Domain::IPV4
            } else {
                socket2::Domain::IPV6
            };
            socket2::Socket::new_raw(
                domain,
                super::flags::creation_type(),
                Some(socket2::Protocol::UDP),
            )
        };
        #[cfg(windows)]
        let original = {
            use windows_sys::Win32::Networking::WinSock::{IPPROTO_UDP, SOCK_DGRAM};
            crate::windows_socket::new(source.is_ipv4(), SOCK_DGRAM, IPPROTO_UDP)
        };
        let original = match original {
            Ok(socket) => socket,
            Err(source) => return Err(self.no_native(&entry, phase("create_socket", source))),
        };
        *entry.lock() = NativeHandleState::Raw(original.into());
        let configured = {
            let native = entry.lock();
            let NativeHandleState::Raw(original) = &*native else {
                unreachable!("same synchronous acquisition owner")
            };
            configure(
                original,
                source,
                preferred_port,
                v6_only,
                interface.as_ref(),
            )
        };
        if let Err(source) = configured {
            let source = Arc::new(source);
            *entry
                .acquisition_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(source.clone());
            let outcome = match entry.close() {
                Ok(receipt) => AcquisitionOutcome::Closed(receipt),
                Err(_) => AcquisitionOutcome::Unknown(entry.id),
            };
            self.reap_confirmed(); // Persist actual confirmation before return.
            return Err(self.acquisition_error(Some(entry.id), outcome, source));
        }
        Ok(ControlledDatagramSocket { entry, interface })
    }
    /// Typed internal registration. The original stays in its entry before
    /// metadata setup and across this consuming runtime boundary.
    pub(super) fn register_native(
        &mut self,
        socket: ControlledDatagramSocket,
    ) -> Result<ControlledDatagramSocket, DatagramAcquisitionError> {
        let entry = &socket.entry;
        let interface = &socket.interface;
        {
            let mut native = entry.lock();
            // from_std consumes the original. Install Unknown before crossing
            // that boundary; neither an error nor unwind fabricates a close.
            let NativeHandleState::Raw(original) = std::mem::replace(
                &mut *native,
                NativeHandleState::Unknown(entry.uncertain.clone()),
            ) else {
                unreachable!("original installed before native registration")
            };
            match tokio::net::UdpSocket::from_std(original) {
                Ok(inner) => {
                    *native = NativeHandleState::Live(TokioDatagramSocket {
                        inner,
                        egress_interface: interface.clone(),
                        observer: None,
                    })
                }
                Err(source) => {
                    let source = phase("register_socket", source);
                    *entry
                        .acquisition_failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(source.clone());
                    *native = NativeHandleState::Unknown(source.clone());
                    return Err(self.acquisition_error(
                        Some(entry.id),
                        AcquisitionOutcome::Unknown(entry.id),
                        source,
                    ));
                }
            }
        }
        Ok(socket)
    }
    fn no_native(
        &mut self,
        entry: &Arc<Entry>,
        source: Arc<std_io::Error>,
    ) -> DatagramAcquisitionError {
        *entry.lock() = NativeHandleState::NoNative;
        self.reap_confirmed();
        self.acquisition_error(Some(entry.id), AcquisitionOutcome::KnownNoNative, source)
    }
}
fn configure(
    original: &std::net::UdpSocket,
    mut source: SocketAddr,
    preferred: Option<u16>,
    v6_only: bool,
    interface: Option<&EgressInterface>,
) -> std_io::Result<()> {
    let socket = socket2::SockRef::from(original);
    super::flags::configure(&socket)
        .map_err(|source| std_io::Error::other(phase("configure_native_flags", source)))?;
    if source.is_ipv6() && v6_only {
        socket
            .set_only_v6(true)
            .map_err(|source| std_io::Error::other(phase("set_only_v6", source)))?;
    }
    match socket.bind(&source.into()) {
        Ok(()) => {}
        Err(error) if preferred.is_some() && error.kind() == std_io::ErrorKind::AddrInUse => {
            // A failed UDP bind leaves this exact original unbound. Retry only
            // its port, retaining source/family/interface; allocate no new fd.
            source.set_port(0);
            socket
                .bind(&source.into())
                .map_err(|source| std_io::Error::other(phase("bind_source", source)))?;
        }
        Err(source) => return Err(std_io::Error::other(phase("bind_source", source))),
    }
    if let Some(interface) = interface {
        crate::egress::bind_udp_to_interface(original, source, interface)
            .map_err(|source| std_io::Error::other(phase("bind_interface", source)))?;
    }
    original
        .set_nonblocking(true)
        .map_err(|source| std_io::Error::other(phase("set_nonblocking", source)))
}
