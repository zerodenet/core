//! Concrete platform originals. No provider trait or caller-supplied factory.
use super::*;
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::component::PacketDeviceIo;
use zero_tun::{OwnedTunIdentity, TunCloseReceipt, TunDevice};

pub(super) struct NativeView {
    pub io: PacketDeviceIo,
    pub interface: Arc<str>,
}
pub(super) struct Original {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    owner: Arc<zero_tun::AdoptedTunOwner>,
    #[cfg(target_os = "windows")]
    owner: Arc<zero_tun::OwnedWintunOwner>,
    issued: Mutex<Option<OwnedTunIdentity>>,
}
impl Original {
    #[cfg(windows)]
    pub(super) fn read_control(&self) -> Option<zero_tun::OwnedWintunReadControl> {
        self.owner.read_control()
    }

    pub(super) fn new(binding: &PacketDeviceBinding) -> Result<Self, &'static str> {
        let valid = match binding {
            PacketDeviceBinding::Descriptor { fd, interface } => {
                cfg!(any(target_os = "linux", target_os = "macos"))
                    && *fd >= 0
                    && !interface.is_empty()
                    && !interface.contains('\0')
            }
            PacketDeviceBinding::Create {
                name,
                addresses,
                mtu,
            } => {
                cfg!(any(
                    target_os = "linux",
                    target_os = "macos",
                    target_os = "windows"
                )) && name
                    .as_ref()
                    .is_none_or(|name| !name.is_empty() && !name.contains('\0'))
                    && !addresses.is_empty()
                    && addresses
                        .iter()
                        .all(|(address, mask)| address.is_ipv4() == mask.is_ipv4())
                    && *mtu > 0
            }
            PacketDeviceBinding::ExistingWindows {
                interface,
                addresses,
                mtu,
            } => {
                cfg!(target_os = "windows")
                    && !interface.is_empty()
                    && !interface.contains('\0')
                    && !addresses.is_empty()
                    && *mtu > 0
            }
        };
        if !valid {
            return Err("invalid or unavailable concrete Packet device binding");
        }
        Ok(Self {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            owner: zero_tun::AdoptedTunOwner::new(),
            #[cfg(target_os = "windows")]
            owner: zero_tun::OwnedWintunOwner::new(),
            issued: Mutex::new(None),
        })
    }
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    pub(super) fn outcome(&self) -> zero_tun::TunAcquisitionOutcome {
        self.owner.outcome()
    }
    #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
    pub(super) fn acquisition_failure(&self) -> Option<Arc<std::io::Error>> {
        self.owner.acquisition_failure()
    }
    pub(super) fn issued(&self) -> Option<OwnedTunIdentity> {
        self.issued
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    #[cfg(target_os = "windows")]
    pub(super) fn prepare_reader(
        &self,
        grant: zero_tun::packet_memory::NativeReadGrant,
        capacity: usize,
    ) -> std::io::Result<zero_tun::OwnedWintunReadWork> {
        self.owner.prepare_read_work_with_capacity(
            grant,
            u32::try_from(capacity)
                .map_err(|_| std::io::Error::other("SDK ring capacity overflow"))?,
        )
    }
    #[cfg(windows)]
    pub(super) fn ring_request_finished(&self) -> bool {
        self.owner.ring_request_finished()
    }
    #[cfg(windows)]
    pub(super) fn session_allocation_returned(&self) -> bool {
        self.owner.session_allocation_returned()
    }
    #[cfg(target_os = "macos")]
    pub(super) fn prepare_macos_workspaces(
        &self,
        read: zero_tun::packet_memory::NativeReadGrant,
        write: zero_tun::packet_memory::NativeReadGrant,
    ) -> std::io::Result<()> {
        self.owner.prepare_macos_workspaces(read, write)
    }
    pub(super) fn stop_reader(&self) {
        #[cfg(target_os = "macos")]
        self.owner.return_unacquired_macos_workspaces();
        #[cfg(target_os = "windows")]
        self.owner.stop();
    }
    pub(super) async fn acquire(
        &self,
        binding: &PacketDeviceBinding,
        component: ComponentId,
    ) -> Result<NativeView, ChannelError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let acquired = match binding {
                PacketDeviceBinding::Descriptor { fd, interface } => {
                    self.owner.acquire(*fd, interface)
                }
                PacketDeviceBinding::Create { name, .. } => self.owner.create(name.as_deref()),
                PacketDeviceBinding::ExistingWindows { .. } => {
                    return Err(error(
                        component,
                        Reason::Unsupported,
                        "Windows original unavailable on this platform",
                    ));
                }
            };
            let io = acquired.map_err(|source| {
                let unknown = matches!(source.outcome, zero_tun::TunAcquisitionOutcome::Unknown(_));
                let mut failure = error(
                    component,
                    Reason::Io,
                    "actual Packet device acquisition failed",
                )
                .with_source(source);
                if unknown {
                    failure.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                }
                failure
            })?;
            *self
                .issued
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(io.owned_identity().clone());
            self.configure_and_wrap(io, binding, component)
        }
        #[cfg(target_os = "windows")]
        {
            let request = match binding {
                PacketDeviceBinding::Create {
                    name,
                    addresses,
                    mtu,
                } => zero_tun::OwnedWintunRequest::Create {
                    name: name.clone(),
                    addresses: addresses.clone(),
                    mtu: *mtu,
                },
                PacketDeviceBinding::ExistingWindows {
                    interface,
                    addresses,
                    mtu,
                } => zero_tun::OwnedWintunRequest::Existing {
                    interface: interface.clone(),
                    addresses: addresses.clone(),
                    mtu: *mtu,
                },
                PacketDeviceBinding::Descriptor { .. } => {
                    return Err(error(
                        component,
                        Reason::Unsupported,
                        "Unix descriptor unavailable on this platform",
                    ));
                }
            };
            // The sole admitted SDK task owns acquisition, configuration,
            // DAD and subsequent RX. Cancellation stops that same original;
            // its actual witness and every native handle remain retained.
            let acquired = self.owner.acquire_in_reader(request).await;
            let io = acquired.map_err(|source| {
                let unknown = matches!(
                    self.owner.outcome(),
                    zero_tun::WintunAcquisitionOutcome::Unknown
                );
                let mut failure = error(component, Reason::Io, "actual Wintun acquisition failed")
                    .with_source(source);
                if unknown {
                    failure.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                }
                failure
            })?;
            *self
                .issued
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(io.owned_identity().clone());
            self.configure_and_wrap(io, binding, component)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            let _ = binding;
            Err(error(
                component,
                Reason::Unsupported,
                "concrete Packet provider unavailable",
            ))
        }
    }
    fn configure_and_wrap<D: TunDevice + 'static>(
        &self,
        io: D,
        binding: &PacketDeviceBinding,
        component: ComponentId,
    ) -> Result<NativeView, ChannelError> {
        #[cfg(not(target_os = "windows"))]
        if let PacketDeviceBinding::Create { addresses, mtu, .. } = binding {
            // The original is already retained and its issued identity recorded.
            // Failure drops only this unique view; close stays with the original.
            io.configure_addresses(addresses, *mtu).map_err(|source| {
                error(
                    component,
                    Reason::Io,
                    "created Packet device configuration failed",
                )
                .with_source(source)
            })?;
        }
        // Windows was fully configured on its same actual SDK worker before
        // its acquisition receipt; do not run configuration again on runtime.
        #[cfg(target_os = "windows")]
        let _ = (binding, component);
        Ok(NativeView {
            interface: Arc::from(io.name()),
            io: Box::pin(io),
        })
    }
    pub(super) fn physical_confirmed(&self, issued: Option<&OwnedTunIdentity>) -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            match self.owner.outcome() {
                zero_tun::TunAcquisitionOutcome::KnownNoNative => issued.is_none(),
                zero_tun::TunAcquisitionOutcome::Closed(receipt) => {
                    issued.is_none_or(|issued| receipt.identity().same_owner(issued))
                }
                zero_tun::TunAcquisitionOutcome::Unknown(_) => false,
            }
        }
        #[cfg(target_os = "windows")]
        {
            match self.owner.outcome() {
                zero_tun::WintunAcquisitionOutcome::KnownNoNative => issued.is_none(),
                zero_tun::WintunAcquisitionOutcome::Closed => issued.is_none_or(|issued| {
                    self.owner
                        .closed_receipt()
                        .is_some_and(|receipt| receipt.identity().same_owner(issued))
                }),
                zero_tun::WintunAcquisitionOutcome::Unknown => false,
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            let _ = issued;
            false
        }
    }
    pub(super) fn receipt(&self) -> Option<Arc<TunCloseReceipt>> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            match self.owner.outcome() {
                zero_tun::TunAcquisitionOutcome::Closed(receipt) => Some(receipt),
                _ => None,
            }
        }
        #[cfg(target_os = "windows")]
        {
            self.owner.closed_receipt()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        None
    }
    pub(super) async fn close(&self) -> Result<Option<Arc<TunCloseReceipt>>, Arc<std::io::Error>> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            match self.owner.outcome() {
                zero_tun::TunAcquisitionOutcome::KnownNoNative => Ok(None),
                zero_tun::TunAcquisitionOutcome::Closed(receipt) => Ok(Some(receipt)),
                zero_tun::TunAcquisitionOutcome::Unknown(_) => self.owner.close().map(Some),
            }
        }
        #[cfg(target_os = "windows")]
        {
            self.owner.close().await
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        Err(Arc::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "concrete Packet provider unavailable",
        )))
    }
}
