//! Native route originals stay in this same state throughout failed SDK work.
use super::*;
use zero_tun::{RouteInstallRequest, SystemLeakGuard, SystemRouteGuard};
pub(super) struct Native {
    pub spec: RouteRuntimeSpec,
    families: Families,
    leak: Option<SystemLeakGuard>,
    _inputs: zero_tun::packet_memory::PacketMetadataCredit,
}
struct Family {
    original: Option<SystemRouteGuard>,
    installed: bool,
}
#[derive(Clone)]
pub(in crate::inbound::tun) struct Snapshot {
    pub exclusions: Vec<IpAddr>,
    pub v4: Option<String>,
    pub v6: Option<String>,
    pub error: Option<String>,
    pub healthy: bool,
    pub changed: bool,
}
impl Native {
    pub(super) fn families_layout(
        count: usize,
    ) -> Result<
        zero_tun::packet_memory::PacketMetadataLayout,
        zero_tun::packet_memory::PacketMemoryError,
    > {
        zero_tun::packet_memory::PacketMetadataLayout::array::<Family>(count)
    }
    pub(super) fn from_prepaid(
        spec: RouteRuntimeSpec,
        credit: zero_tun::packet_memory::PacketMetadataCredit,
        inputs: zero_tun::packet_memory::PacketMetadataCredit,
    ) -> Result<Self, ChannelError> {
        let expected = Self::families_layout(spec.addresses.len()).map_err(|source| {
            error(Reason::Capacity, "route family layout overflow").with_source(source)
        })?;
        if credit.layout() != expected {
            return Err(error(Reason::Internal, "route family credit mismatch"));
        }
        let actual = zero_tun::packet_memory::PacketMetadataLayout::array::<(IpAddr, IpAddr)>(
            spec.addresses.capacity(),
        )
        .and_then(|layout| {
            layout.checked_add(zero_tun::packet_memory::PacketMetadataLayout::array::<
                ipnet::IpNet,
            >(spec.include_cidrs.capacity())?)
        })
        .and_then(|layout| {
            layout.checked_add(zero_tun::packet_memory::PacketMetadataLayout::array::<
                ipnet::IpNet,
            >(spec.exclude_cidrs.capacity())?)
        })
        .and_then(|layout| {
            layout.checked_add(zero_tun::packet_memory::PacketMetadataLayout::array::<u8>(
                spec.recovery_key.capacity(),
            )?)
        })
        .map_err(|source| {
            error(Reason::Capacity, "actual route input layout overflow").with_source(source)
        })?;
        if inputs.layout() != actual || spec.tun_name.capacity() != 0 {
            return Err(error(
                Reason::Internal,
                "route input allocations do not match their prepaid layout",
            ));
        }
        let mut families = Box::<[Family]>::new_uninit_slice(spec.addresses.len());
        for family in &mut families {
            family.write(Family {
                original: None,
                installed: false,
            });
        }
        Ok(Self {
            spec,
            families: Families {
                values: unsafe { families.assume_init() },
                _credit: credit,
            },
            leak: None,
            _inputs: inputs,
        })
    }
    pub fn install(
        &mut self,
        name: String,
        exclusions: &[IpAddr],
        egress: &zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Snapshot> {
        self.spec.tun_name = name;
        self.reconcile(exclusions, egress)
    }
    pub fn reconcile(
        &mut self,
        exclusions: &[IpAddr],
        egress: &zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Snapshot> {
        let mut changed = false;
        let mut failed = None;
        let mark = self
            .spec
            .strict_route
            .then(|| strict_route_socket_mark(&self.spec.recovery_key));
        for ((address, netmask), slot) in self.spec.addresses.iter().zip(self.families.iter_mut()) {
            let result = if slot.installed {
                slot.original
                    .as_mut()
                    .expect("same installed family")
                    .reconcile(exclusions)
            } else {
                // Partial installation cannot be overwritten by a retry. Close
                // its exact guard first; failure preserves it in this slot.
                if let Some(original) = slot.original.as_mut() {
                    original.close_retained()?;
                    slot.original.take();
                }
                let captured = zero_tun::capture_route_prefixes_with_exclusions(
                    *address,
                    &self.spec.include_cidrs,
                    &self.spec.exclude_cidrs,
                );
                SystemRouteGuard::install_retained_with_egress(
                    &mut slot.original,
                    RouteInstallRequest {
                        tun_name: &self.spec.tun_name,
                        recovery_key: &self.spec.recovery_key,
                        address: *address,
                        netmask: *netmask,
                        captured: &captured,
                        excluded: exclusions,
                    },
                    |state| publish_family_egress(egress, address.is_ipv6(), state, mark),
                )
                .map(|()| {
                    slot.installed = true;
                    true
                })
            };
            match result {
                Ok(actual) => {
                    changed |= actual;
                    let original = slot.original.as_ref().expect("same installed family");
                    publish_family_egress(
                        egress,
                        address.is_ipv6(),
                        original.family_egress(),
                        mark,
                    )?;
                    log_route_install_carrier_selected(
                        original,
                        egress.generation(),
                        "source_owned_reconcile",
                    );
                }
                Err(error) if self.spec.strict_route => return Err(error),
                Err(error) => {
                    failed.get_or_insert_with(|| error.to_string());
                }
            }
        }
        let protected = self
            .spec
            .addresses
            .iter()
            .flat_map(|(address, _)| {
                zero_tun::capture_route_prefixes_with_exclusions(
                    *address,
                    &self.spec.include_cidrs,
                    &self.spec.exclude_cidrs,
                )
            })
            .collect::<Vec<_>>();
        if self.spec.strict_route {
            if let Some(original) = self.leak.as_mut() {
                changed |= original.reconcile(&protected, exclusions)?;
            } else {
                SystemLeakGuard::install_retained(
                    &mut self.leak,
                    &self.spec.tun_name,
                    &self.spec.recovery_key,
                    &protected,
                    exclusions,
                )?;
                changed = true;
            }
        }
        let name = |ipv6| {
            self.families
                .iter()
                .filter(|slot| slot.installed)
                .filter_map(|slot| slot.original.as_ref())
                .find(|original| original.is_ipv6() == ipv6)
                .and_then(|original| original.family_egress().available_interface())
                .map(|interface| interface.name().to_owned())
        };
        Ok(Snapshot {
            exclusions: exclusions.to_vec(),
            v4: name(false),
            v6: name(true),
            healthy: failed.is_none() && self.families.iter().all(|slot| slot.installed),
            error: failed,
            changed,
        })
    }
    pub fn close(&mut self) -> io::Result<()> {
        let mut failures = Vec::new();
        if let Some(original) = self.leak.as_mut() {
            match original.close_retained() {
                Ok(()) => {
                    self.leak.take();
                }
                Err(error) => {
                    failures.push(error);
                }
            }
        }
        for slot in self.families.iter_mut().rev() {
            if let Some(original) = slot.original.as_mut() {
                match original.close_retained() {
                    Ok(()) => {
                        slot.original.take();
                        slot.installed = false;
                    }
                    Err(error) => {
                        failures.push(error);
                    }
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(NativeCloseFailures(failures)))
        }
    }
}

#[derive(Debug)]
struct NativeCloseFailures(Vec<io::Error>);
impl std::fmt::Display for NativeCloseFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            source.fmt(f)?;
        }
        Ok(())
    }
}
impl std::error::Error for NativeCloseFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source as _)
    }
}

// Concrete array owner; actual Box deallocates BEFORE its exact layout refunds.
struct Families {
    values: Box<[Family]>,
    _credit: zero_tun::packet_memory::PacketMetadataCredit,
}
impl std::ops::Deref for Families {
    type Target = [Family];
    fn deref(&self) -> &[Family] {
        &self.values
    }
}
impl std::ops::DerefMut for Families {
    fn deref_mut(&mut self) -> &mut [Family] {
        &mut self.values
    }
}
