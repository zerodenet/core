//! Native source-preserving UDP authority extracted by the actual host factory.
//! The immutable dial policy belongs to that exact component recipe. Replacing
//! it replaces its owner; no business Session or second UDP pool is created.
use super::*;
use std::net::SocketAddr;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::{
    EgressSelection,
    contract::{
        CancellationToken,
        component::DatagramBindingAuthorization,
        scope::{TaskRegistrar, TaskScopeIdentity},
    },
};
use zero_traits::DialPolicy;

struct NativeSource {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    cancel: CancellationToken,
    policy: DialPolicy,
    egress: EgressInterfaceControl,
    generation: NetworkGeneration,
}
impl BackgroundNetwork {
    /// The host passes its actual BuildContext registrar/cancellation and cold
    /// recipe policy. Only HostResources can issue the matching ordinal's token;
    /// protocol ResourceAccess exposes activation/opening, never authorization.
    pub(crate) fn bound_datagram_source(
        &self,
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
        cancel: CancellationToken,
        policy: DialPolicy,
        generation: NetworkGeneration,
    ) -> Result<Arc<dyn DatagramBindingAuthorization>, ChannelError> {
        policy.validate().map_err(|source| {
            invalid(Reason::InvalidConfig, "invalid host bound UDP dial policy").with_source(source)
        })?;
        let source = NativeSource {
            instance,
            scope: tasks.scope_identity(),
            cancel,
            policy,
            egress: self.egress.clone(),
            generation,
        };
        source.check_current(generation)?;
        Ok(Arc::new(source))
    }
}
impl NativeSource {
    fn error(&self, reason: Reason, message: &'static str) -> ChannelError {
        let mut failure = invalid(reason, message);
        failure.component = Some(self.instance.component);
        failure
    }
}
impl DatagramBindingAuthorization for NativeSource {
    fn policy(&self) -> &DialPolicy {
        &self.policy
    }
    fn preferred_port(&self) -> Option<u16> {
        None
    }
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError> {
        // Issuance is cold. The actual native provider separately requires this
        // same Scope to be open and its actual driver installed before I/O.
        if self.cancel.is_cancelled() {
            return Err(self.error(Reason::Revoked, "actual host bound UDP owner stopped"));
        }
        if expected != self.generation || expected.0 != self.egress.generation() {
            return Err(self.error(Reason::StalePlan, "actual bound UDP network changed"));
        }
        Ok(())
    }
    fn select(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<EgressSelection, ChannelError> {
        self.check_current(expected)?;
        if !self.scope.resource_open() {
            return Err(self.error(Reason::Revoked, "actual bound UDP Scope is not open"));
        }
        let selection = self
            .egress
            .select_for_peer_with_policy(peer, &self.policy)
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
