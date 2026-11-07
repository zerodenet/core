//! Reuse only a live original Source whose full installed input is unchanged.
use super::*;
/// Same finite declaration's actual Windows SDK branch only. Identity/class
/// is not native acquisition, task registration, or a closed receipt.
pub(super) fn requested_ring_declaration(
    resource: &zero_platform_tokio::contract::component::ResourceDeclaration,
) -> bool {
    use zero_platform_tokio::contract::component::{PacketDeviceBinding, ResourceDeclaration};
    cfg!(windows)
        && matches!(
            resource,
            ResourceDeclaration::ControlledPacketDevice {
                binding: PacketDeviceBinding::Create { .. }
                    | PacketDeviceBinding::ExistingWindows { .. },
                ..
            }
        )
}
pub(crate) fn input_identity(input: &zero_config::InboundConfig) -> Result<[u8; 32], EngineError> {
    use sha2::{Digest, Sha256};
    // Opaque configuration hashing only: the host does not parse protocol
    // settings or retain another configuration/profile or resource owner.
    let encoded = serde_json::to_vec(input).map_err(std::io::Error::other)?;
    Ok(Sha256::digest(encoded).into())
}
impl InboundOwnerRegistry {
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    pub(in crate::runtime) fn packet_source_memory(
        &self,
        input: &zero_config::InboundConfig,
    ) -> Result<Option<zero_platform_tokio::contract::packet::PacketSourceMemory>, EngineError>
    {
        let expected = input_identity(input)?;
        let owner = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&input.tag)
            .cloned();
        Ok(owner
            .filter(|owner| owner.is_running() && owner.source_input.get() == Some(&expected))
            .and_then(|owner| owner.packet_native.get().and_then(std::sync::Weak::upgrade))
            .and_then(|grant| grant.packet_memory()))
    }

    pub(in crate::runtime) fn packet_source_origin(
        &self,
        input: &zero_config::InboundConfig,
    ) -> Result<
        Option<zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin>,
        EngineError,
    > {
        #[cfg(feature = "datagram-route-runtime")]
        {
            let expected = input_identity(input)?;
            let owner = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&input.tag)
                .cloned();
            if let Some(owner) = owner
                .filter(|owner| owner.is_running() && owner.source_input.get() == Some(&expected))
            {
                return Ok(owner
                    .packet_native
                    .get()
                    .and_then(std::sync::Weak::upgrade)
                    .and_then(|original| original.original_origin()));
            }
        }
        let _ = input;
        Ok(None)
    }
}

#[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
impl InboundOwnerRegistry {
    pub(in crate::runtime) fn packet_driver_candidate(
        &self,
        protocols: &ProtocolInventory,
        grant: &zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
    ) -> Result<
        Option<(
            Arc<zero_config::RuntimeConfig>,
            Arc<crate::component::catalog::ComponentPlan>,
        )>,
        zero_platform_tokio::contract::ChannelError,
    > {
        let denied = || {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                zero_platform_tokio::contract::Impact::Component,
            )
        };
        // Real initializing Sources are retained in reservations before their
        // main/wire can poll. Clone under each lock and release before getters.
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        entries.extend(
            self.reservations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .values()
                .cloned(),
        );
        for owner in entries {
            let matches = owner
                .packet_native
                .get()
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|original| original.matches_packet_driver(grant));
            if !matches {
                continue;
            }
            let tag = owner.source_tag.get().ok_or_else(denied)?;
            let input = owner.source_input.get().ok_or_else(denied)?;
            let dependencies = owner.source_dependencies.get().ok_or_else(denied)?;
            let (config, plan) = protocols
                .runtime_host()
                .components()
                .packet_source_driver_candidate(tag, input, dependencies)?;
            let actual = config
                .inbounds
                .iter()
                .find(|actual| actual.tag == tag.as_ref())
                .ok_or_else(denied)?;
            if input_identity(actual).map_err(|source| denied().with_source(source))? != *input {
                return Err(denied());
            }
            return Ok(Some((config, plan)));
        }
        Ok(None)
    }
}

#[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
impl InboundOwnerRegistry {
    /// Match the actual installed/preparing Source, not a second native cell.
    /// This projects lineage only; neither registration nor projection is Ready.
    pub(in crate::runtime) fn packet_carrier_source(
        &self,
        input: &zero_config::InboundConfig,
        plan: &crate::component::catalog::ComponentPlan,
        native: &zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        memory: &zero_platform_tokio::contract::packet::PacketSourceMemory,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, EngineError> {
        let denied =
            || std::io::Error::other("Packet Source input, dependency, Scope or original changed");
        let expected = input_identity(input)?;
        let dependencies = plan
            .source_dependency_instances(&input.tag, &expected)
            .map_err(std::io::Error::other)?;
        let mut owners = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&input.tag)
            .cloned()
            .into_iter()
            .collect::<Vec<_>>();
        owners.extend(
            self.reservations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .values()
                .filter(|owner| {
                    owner
                        .source_tag
                        .get()
                        .is_some_and(|tag| tag.as_ref() == input.tag)
                })
                .cloned(),
        );
        for owner in owners {
            let Some(grant) = owner.packet_native.get().and_then(std::sync::Weak::upgrade) else {
                continue;
            };
            let Some(actual) = grant.original_origin() else {
                continue;
            };
            if !actual.same_original(native) {
                continue;
            }
            if !owner.matches_packet_source_registration(native)
                || owner.source_input.get() != Some(&expected)
                || !owner
                    .source_dependencies
                    .get()
                    .is_some_and(|actual| actual.as_ref() == dependencies.as_slice())
                || !grant
                    .packet_memory()
                    .is_some_and(|actual| actual.view.same_original(&memory.view))
            {
                return Err(denied().into());
            }
            let source = grant.packet_source_origin().ok_or_else(denied)?;
            let same = source.same_original(
                &zero_platform_tokio::controlled_carrier::CarrierOrigin::from_listener(
                    native.clone(),
                ),
            ) || source
                .logical_packet()
                .and_then(|logical| logical.source_admission())
                .is_some_and(|actual| actual.same_original(native));
            if !same || source.is_retired() {
                return Err(denied().into());
            }
            return Ok(source);
        }
        Err(denied().into())
    }
}
