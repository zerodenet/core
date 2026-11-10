//! Actual per-call owning role/provider observation, without allocating a graph walk.
use super::*;
use zero_platform_tokio::{
    contract::component::ResourceDeclaration as R, controlled_carrier::ProtocolOriginal,
};
impl ComponentPlan {
    pub fn validate_protocol_original(
        &self,
        role: zero_core::component::RoleInstanceId,
        original: &ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let fail = || {
            invalid(
                Reason::StalePlan,
                "protocol original has no matching actual owning role/provider/Scope",
            )
        };
        // This contract is the installed per-call owning role, not permission
        // to retire an anonymous shared pool through a mere dependency path.
        let owning = self.role_slot(role).ok_or_else(fail)?;
        // Browser control sockets belong to the actual roleless provider,
        // which is a direct canonical dependency of this owning role. This
        // branch still requires its exact installed receiver below; a numeric
        // dependency, listener declaration or equal Scope cannot issue it.
        let slot = if original.accepted().is_some() && owning.instance_id() != original.instance() {
            if !owning
                .recipe
                .spec()
                .dependencies
                .contains(&original.instance().component)
            {
                return Err(fail());
            }
            self.order
                .iter()
                .find(|slot| {
                    slot.instance_id() == original.instance() && slot.recipe.spec().roles.is_empty()
                })
                .ok_or_else(fail)?
        } else {
            &owning
        };
        if slot.instance_id() != original.instance()
            || !slot.matches_scope_identity(Some(original.scope_identity()))
        {
            return Err(fail());
        }
        let matched = slot.recipe.spec().resources.iter().enumerate().any(|(ordinal, declaration)| {
            if let Some(metadata) = original.metadata() {
                matches!(declaration, R::ControlledMetadataCarrier { dial, .. } if dial == &original.binding().dial)
                    && slot.original_metadata_owner(ordinal).is_some_and(|actual| actual.same_owner(metadata.owner()) && actual.instance() == original.instance())
            } else if let Some(adoption) = original.adoption() {
                (matches!(declaration, R::ControlledServiceDatagram { dial, .. } | R::ControlledCarrierPool { dial, datagram: true, .. } if dial == &original.binding().dial)
                    || cached::installed(&slot) && matches!(declaration,R::ControlledCarrierPool {dial,datagram:false,..} if dial==&original.binding().dial))
                    && slot.original_carrier_owner(ordinal).is_some_and(|actual| actual.matches_adoption(adoption) && actual.instance() == original.instance() && actual.bound().is_some_and(|bound| bound.scope_identity().same_scope(original.scope_identity())))
            } else if let Some(accepted) = original.accepted() {
                matches!(declaration, R::TcpListener { accepted: Some(_), .. })
                    && slot.original_accepted_receiver(ordinal).is_some_and(|receiver| accepted.matches_receiver(&receiver))
            } else { false }
        });
        if matched {
            Ok(())
        } else {
            Err(fail())
        }
    }
    /// Finite data-only capacity of these same actual owning roles. The
    /// retained table still prepays its actual allocation before growth.
    pub fn protocol_original_capacity(
        &self,
        roles: &[zero_core::component::RoleInstanceId],
    ) -> Result<usize, ChannelError> {
        let mut capacity = 0usize;
        for (index, role) in roles.iter().enumerate() {
            // Repeated positions in a legal path still share one actual
            // provider's original quota; no Vec/Set or closure snapshot.
            if roles[..index].contains(role) {
                continue;
            }
            let slot = self
                .role_slot(*role)
                .ok_or_else(|| invalid(Reason::StalePlan, "pending owning role changed"))?;
            for dependency in &slot.recipe.spec().dependencies {
                if roles[..index].iter().any(|previous| {
                    self.role_slot(*previous).is_some_and(|previous| {
                        previous.recipe.spec().dependencies.contains(dependency)
                    })
                }) {
                    continue;
                }
                let Some(provider) = self.order.iter().find(|provider| {
                    provider.instance_id().component == *dependency
                        && provider.recipe.spec().roles.is_empty()
                }) else {
                    continue;
                };
                for (ordinal, declaration) in provider.recipe.spec().resources.iter().enumerate() {
                    if let R::TcpListener {
                        accepted: Some(limits),
                        ..
                    } = declaration
                    {
                        if provider.original_accepted_receiver(ordinal).is_some() {
                            capacity = capacity.checked_add(limits.max_sockets.get()).ok_or_else(
                                || {
                                    invalid(
                                        Reason::Capacity,
                                        "pending accepted original capacity overflow",
                                    )
                                },
                            )?;
                        }
                    }
                }
            }
            for declaration in &slot.recipe.spec().resources {
                let limit = match declaration {
                    R::ControlledMetadataCarrier { max_origins, .. } => max_origins.get(),
                    R::ControlledServiceDatagram { max_sockets, .. }
                    | R::ControlledCarrierPool { max_sockets, .. } => max_sockets.get(),
                    _ => 0,
                };
                capacity = capacity.checked_add(limit).ok_or_else(|| {
                    invalid(Reason::Capacity, "pending protocol capacity overflow")
                })?;
            }
        }
        Ok(capacity)
    }
}
