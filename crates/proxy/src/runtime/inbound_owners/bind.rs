//! One actual configuration's pre-bind owners, retained before native creation.
mod grant;
#[cfg(feature = "raw-ip-runtime")]
mod icmp;
mod lease;
pub(super) mod native;
mod retirement;
use super::*;
use crate::protocol_registry::PreparedInboundBind;
pub(crate) use lease::{InboundBindLease, LeasedInbound};
pub(crate) use native::NativeListenerView;
use std::{collections::BTreeMap, sync::Weak};
use zero_config::RuntimeConfig;

#[derive(Default)]
pub(super) struct BindReservations {
    pub(super) closing: bool,
    config: Weak<RuntimeConfig>,
    pub(super) entries: BTreeMap<String, Arc<InboundOwner>>,
}
impl InboundOwnerRegistry {
    pub(crate) fn reserve_listener(
        self: &Arc<Self>,
        tag: &str,
        config: &Arc<RuntimeConfig>,
        protocols: &ProtocolInventory,
        mut prepared: PreparedInboundBind,
        shutdown: watch::Sender<bool>,
        max_bind_attempts: NonZeroUsize,
    ) -> Result<InboundBindLease, EngineError> {
        if !config.inbounds.iter().any(|inbound| inbound.tag == tag) {
            return Err(io(
                "listener reservation is outside its actual configuration",
            ));
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            let input = config
                .inbounds
                .iter()
                .find(|input| input.tag == tag)
                .expect("checked actual source");
            prepared = prepared
                .with_actual_dependencies(protocols.source_dependency_instances(config, input)?)?;
        }
        let dependencies = prepared.actual_dependencies();
        let accepted_slots = prepared
            .accepted_tcp_slots()
            .ok_or_else(|| io("accepted TCP task ceiling overflow"))?;
        let limit = prepared
            .internal_tasks()
            .checked_add(1)
            .and_then(|limit| limit.checked_add(prepared.host_tasks()))
            .and_then(|limit| limit.checked_add(accepted_slots))
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| io("inbound declared task limit exhausted"))?;
        let mut reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reserved.closing {
            return Err(super::preparing::stopped_preparation());
        }
        if !reserved.entries.is_empty() && !reserved.config.ptr_eq(&Arc::downgrade(config)) {
            return Err(io(
                "previous listener reservation batch still requires confirmed cleanup",
            ));
        }
        if reserved.entries.contains_key(tag) {
            return Err(io("this listener already has a retained bind reservation"));
        }
        self.mark_listener_generation();
        let identity = protocols.allocate_resource_identity()?;
        let owner = Arc::new(InboundOwner::new(
            identity,
            limit,
            shutdown.clone(),
            protocols
                .runtime_host()
                .original_budgets()
                .component_tasks
                .clone(),
        )?);
        #[cfg(feature = "raw-ip-runtime")]
        let requested_ring = prepared
            .resources()
            .iter()
            .any(super::packet_source::requested_ring_declaration)
            || protocols.source_requested_ring_dependencies(config, &dependencies)?;
        #[cfg(not(feature = "raw-ip-runtime"))]
        let requested_ring = false;
        owner
            .requested_ring_source_class
            .set(requested_ring)
            .map_err(|_| io("actual Source SDK declaration class already installed"))?;
        owner
            .source_dependencies
            .set(dependencies)
            .map_err(|_| io("actual Source dependencies already installed"))?;
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(input) = config.inbounds.iter().find(|input| input.tag == tag) {
            owner
                .source_input
                .set(super::packet_source::input_identity(input)?)
                .expect("one original Source input");
        }

        if let Some(recovery) = prepared.take_recovery() {
            owner.install_recovery(recovery)?;
        }
        let grant = Arc::new(
            native::NativeBindGrant::new(
                identity,
                prepared.resources(),
                prepared.address(),
                max_bind_attempts,
                owner.task_access().cancellation(),
                owner.accepted_registrar(),
                protocols.runtime_host().original_budgets(),
                Arc::downgrade(&owner),
            )
            .map_err(std::io::Error::other)?,
        );
        #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
        if let native::NativeBindGrant::Datagram(original) = grant.as_ref() {
            if grant.needs_packet_reads() {
                owner
                    .packet_native
                    .set(Arc::downgrade(original))
                    .map_err(|_| io("actual packet listener was already installed"))?;
                // Install the same controller before BoundResources snapshots
                // it or the first workspace/native bind can run. Actual main
                // and wire registrations still supply their own participants.
                owner.prepare_packet_source_pause();
            }
        }
        // Construction above is cold. The unique actual native holder is a
        // registered resource before this owner or any bind capability escapes.
        owner.install_listener_native(identity, grant.resource())?;
        for receiver in grant.accepted_task_receiver() {
            owner.install_accepted_receiver(receiver)?;
        }
        for resource in grant.extra_resources() {
            owner.install_listener_native(identity, resource)?;
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let [zero_platform_tokio::contract::component::ResourceDeclaration::DatagramListener {
            ..
        }, declaration @ zero_platform_tokio::contract::component::ResourceDeclaration::NativeIcmp { .. }, zero_platform_tokio::contract::component::ResourceDeclaration::PacketDatagramReads { datagram: 0, .. }] =
            prepared.resources()
        {
            owner.install_icmp_probe(declaration, protocols.runtime_host().original_budgets())?;
        }
        reserved.config = Arc::downgrade(config);
        reserved.entries.insert(tag.to_owned(), owner.clone());
        Ok(InboundBindLease::new(
            Arc::downgrade(self),
            tag.to_owned(),
            protocols.clone(),
            prepared,
            owner,
            grant,
            shutdown,
        ))
    }
    pub(crate) fn open_bind_admission(&self) -> Result<(), EngineError> {
        let active = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !active.is_empty() || !reserved.entries.is_empty() {
            return Err(io("old listener owners still require confirmed cleanup"));
        }
        reserved.closing = false;
        Ok(())
    }
    pub(super) fn close_bind_admission(&self) {
        self.reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closing = true;
    }
}
fn io(message: &'static str) -> EngineError {
    std::io::Error::other(message).into()
}
#[cfg(test)]
pub(super) mod tests;
