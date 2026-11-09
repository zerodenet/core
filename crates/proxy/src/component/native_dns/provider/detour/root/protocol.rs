//! The same actual Query anchor receives a sealed protocol original before IO.
use super::*;
use zero_platform_tokio::controlled_carrier::ProtocolOriginal;
impl Root {
    pub(in crate::component::native_dns::provider) fn bind_query_entry(
        &self,
        entry: &Arc<custody::NativeUse>,
    ) -> std::io::Result<()> {
        if !entry.task.same_registration(&self.caller) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "DNS protocol anchor belongs to a different actual Query",
            ));
        }
        let actual = self.query_entry.get_or_init(|| Arc::downgrade(entry));
        if !actual.ptr_eq(&Arc::downgrade(entry)) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "DNS root cannot replace its actual Query row",
            ));
        }
        Ok(())
    }
    pub(in crate::component::native_dns::provider) fn query_transport(
        self: &Arc<Self>,
    ) -> UpstreamTransport {
        UpstreamTransport::new(self.binding.clone(), self.clone())
    }
    pub(super) fn retain_query_protocol(
        &self,
        binding: &UpstreamBinding,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let fail = || {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "DNS protocol original differs from the actual Query/role/Scope/prefix",
            )
        };
        let observer = self.waiting.get().ok_or_else(fail)?;
        if binding != &self.binding
            || !original.matches_observer(observer)
            || original.binding().network_generation != self.binding.network_generation
            || self.egress.generation() != self.binding.network_generation.0
            || self.cancel.is_cancelled()
            || self.caller.is_joined()
            || !zero_dns::OwnedDnsQueryTasks::matches_current_caller(&self.caller)
        {
            return Err(fail());
        }
        let source = self.current.upgrade().ok_or_else(fail)?;
        let services = source.runtime_services().map_err(error)?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|source| {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::Revoked,
                "DNS protocol plan retired",
            )
            .with_source(source.clone())
        })?;
        let selected = self.roles.iter().enumerate().any(|(index, role)| {
            original.binding().prefix == self.roles[..index]
                && plan.validate_protocol_original(*role, &original).is_ok()
        });
        if !selected {
            return Err(fail());
        }
        // The actual BackgroundCarrier already authorized acquisition before
        // issuing this sealed value. Receiving its metadata grants no I/O and
        // must not allocate/register a second copy of that dependency graph.
        let ceiling = plan.protocol_original_capacity(&self.roles)?;
        let entry = self
            .query_entry
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(fail)?;
        if !entry.task.same_registration(&self.caller) {
            return Err(fail());
        }
        let memory = self.receiver.metadata_memory()?;
        entry.retain_protocol_original(original.clone(), &memory, ceiling)?;
        if let Some(metadata) = original.metadata() {
            zero_dns::OwnedDnsQueryTasks::record_metadata(metadata).map_err(error)?;
        }
        Ok(())
    }
}
