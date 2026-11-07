//! Concrete native binding installed by the host before erasing ResourceAccess.
//! No protocol can supply a policy callback through its BuildContext.
use super::*;
use crate::component::instance::HostResources;
use zero_core::component::{ComponentInstanceId, NetworkGeneration};
use zero_platform_tokio::{
    EgressInterfaceControl, EgressSelection,
    contract::{
        CancellationToken, ChannelError, Reason,
        component::{DatagramBindingAuthorization, ResourceDeclaration},
        scope::{TaskRegistrar, TaskScopeIdentity},
    },
};
use zero_traits::DialPolicy;

#[derive(Clone)]
pub(super) struct NativeBinding {
    ordinal: usize,
    egress: EgressInterfaceControl,
    policy: DialPolicy,
    generation: NetworkGeneration,
}
struct Source {
    binding: NativeBinding,
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    cancel: CancellationToken,
}
impl ComponentRecipe {
    /// Cold host assembly only. The policy is part of this recipe's immutable
    /// identity; a change must select a fresh component generation.
    pub fn with_native_datagram_binding(
        mut self,
        ordinal: usize,
        egress: EgressInterfaceControl,
        policy: DialPolicy,
        generation: NetworkGeneration,
    ) -> Result<Self, ChannelError> {
        if !matches!(
            self.spec.resources.get(ordinal),
            Some(ResourceDeclaration::BoundOutboundDatagram { .. })
        ) || self
            .bound_datagrams
            .iter()
            .any(|binding| binding.ordinal == ordinal)
        {
            return Err(super::super::instance::invalid(
                Reason::InvalidConfig,
                "native UDP binding requires one exact declared ordinal",
            ));
        }
        policy.validate().map_err(|source| {
            super::super::instance::invalid(Reason::InvalidConfig, "invalid host UDP dial policy")
                .with_source(source)
        })?;
        // The actual factory's spec already includes these native facts. Do
        // not rewrite only the outer recipe identity: cold reuse compares the
        // same factory spec and must preserve existing active TCP continuity.
        let mut bindings = self.bound_datagrams.to_vec();
        bindings.push(NativeBinding {
            ordinal,
            egress,
            policy,
            generation,
        });
        self.bound_datagrams = bindings.into();
        Ok(self)
    }
    pub(in crate::component) fn bind_native_datagrams(
        &self,
        instance: ComponentInstanceId,
        resources: &HostResources,
        tasks: &TaskRegistrar,
        cancel: CancellationToken,
    ) -> Result<(), ChannelError> {
        for binding in self.bound_datagrams.iter() {
            resources.issue_bound_datagram_binding(
                binding.ordinal,
                Arc::new(Source {
                    binding: binding.clone(),
                    instance,
                    scope: tasks.scope_identity(),
                    cancel: cancel.clone(),
                }),
                binding.generation,
                tasks,
            )?;
        }
        Ok(())
    }
}
impl Source {
    fn error(&self, reason: Reason, message: &'static str) -> ChannelError {
        let mut failure = super::super::instance::invalid(reason, message);
        failure.component = Some(self.instance.component);
        failure
    }
}
impl DatagramBindingAuthorization for Source {
    fn policy(&self) -> &DialPolicy {
        &self.binding.policy
    }
    fn preferred_port(&self) -> Option<u16> {
        None
    }
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError> {
        // Issuance is cold. Opening separately requires the original Scope and
        // registered actual driver; cold state must not reject valid recipes.
        if self.cancel.is_cancelled() {
            return Err(self.error(Reason::Revoked, "actual host UDP owner stopped"));
        }
        if expected != self.binding.generation || expected.0 != self.binding.egress.generation() {
            return Err(self.error(Reason::StalePlan, "actual bound UDP network changed"));
        }
        Ok(())
    }
    fn select(
        &self,
        peer: std::net::SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<EgressSelection, ChannelError> {
        self.check_current(expected)?;
        if !self.scope.resource_open() {
            return Err(self.error(Reason::Revoked, "actual bound UDP Scope is not open"));
        }
        let selection = self
            .binding
            .egress
            .select_for_peer_with_policy(peer, &self.binding.policy)
            .map_err(|source| {
                self.error(Reason::Io, "actual bound UDP egress selection failed")
                    .with_source(source)
            })?;
        self.check_current(expected)?;
        if selection.generation() != expected.0 {
            return Err(self.error(Reason::StalePlan, "bound UDP selection changed network"));
        }
        Ok(selection)
    }
}
