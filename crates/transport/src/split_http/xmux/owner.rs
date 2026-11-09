use super::*;
// A selection partition, not IO/caller/retirement authority.
#[derive(Clone)]
pub(super) struct OwnerKey(zero_traits::packet_metadata::PacketMetadataArc<Owner>);
pub(super) struct Owner {
    pub(super) instance: zero_core::component::ComponentInstanceId,
    pub(super) scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    pub(super) binding: zero_platform_tokio::contract::network::UpstreamBinding,
    pub(super) memory: zero_platform_tokio::contract::component::PacketDriverMetadataView,
}
impl std::ops::Deref for OwnerKey {
    type Target = Owner;
    fn deref(&self) -> &Owner {
        &self.0
    }
}
impl OwnerKey {
    pub(super) fn same(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.scope.same_scope(&other.scope)
            && self.binding == other.binding
    }
    pub(super) fn prepare(
        instance: zero_core::component::ComponentInstanceId,
        scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
        memory: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> io::Result<Self> {
        use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};
        let interface = binding.dial.interface.as_deref().unwrap_or("");
        let layout = PacketMetadataArc::<Owner>::layout()
            .and_then(|layout| {
                layout.checked_add(PacketMetadataLayout::array::<
                    zero_core::component::RoleInstanceId,
                >(binding.prefix.len())?)
            })
            .and_then(|layout| {
                layout.checked_add(PacketMetadataLayout::array::<u8>(interface.len())?)
            })
            .map_err(io::Error::other)?;
        let credit = memory.reserve(layout).map_err(io::Error::other)?;
        let prefix =
            Box::<[zero_core::component::RoleInstanceId]>::new_uninit_slice(binding.prefix.len());
        let pointer = Box::into_raw(prefix) as *mut zero_core::component::RoleInstanceId;
        // SAFETY: exact promised array and no live elements yet.
        let mut prefix = unsafe { Vec::from_raw_parts(pointer, 0, binding.prefix.len()) };
        prefix.extend_from_slice(&binding.prefix);
        let interface = if binding.dial.interface.is_some() {
            let mut bytes = Box::<[u8]>::new_uninit_slice(interface.len());
            for (slot, byte) in bytes.iter_mut().zip(interface.bytes()) {
                slot.write(byte);
            }
            // SAFETY: every byte initialized from the existing valid UTF-8 name.
            let bytes = unsafe { bytes.assume_init() }.into_vec();
            Some(String::from_utf8(bytes).expect("same valid interface name"))
        } else {
            None
        };
        let binding = zero_platform_tokio::contract::network::UpstreamBinding {
            prefix,
            network_generation: binding.network_generation,
            dial: zero_traits::DialPolicy {
                address_family: binding.dial.address_family,
                source_ip: binding.dial.source_ip,
                interface,
            },
        };
        PacketMetadataArc::from_prepaid(
            Owner {
                instance,
                scope,
                binding,
                memory,
            },
            credit,
        )
        .map(Self)
        .map_err(|(source, _, _)| io::Error::other(source))
    }
}
