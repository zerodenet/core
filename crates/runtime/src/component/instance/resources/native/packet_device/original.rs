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
    pub(super) fn acquire(
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
            let acquired = match binding {
                PacketDeviceBinding::Create { name, .. } => self.owner.create(name.as_deref()),
                PacketDeviceBinding::ExistingWindows {
                    interface,
                    addresses,
                    mtu,
                } => self.owner.open_existing(interface, addresses, *mtu),
                PacketDeviceBinding::Descriptor { .. } => {
                    return Err(error(
                        component,
                        Reason::Unsupported,
                        "Unix descriptor unavailable on this platform",
                    ));
                }
            };
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
