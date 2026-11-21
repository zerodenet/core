use super::{UdpAssociationCloseKind, UdpRuntimeServices};
mod control;
/// Actual completion statistics only. No network, registry or retirement owner
/// is retained by a controller living inside RetiredDatagrams.
#[derive(Clone)]
pub(crate) struct UdpAssociationObserver {
    host: std::sync::Weak<dyn AssociationMetrics>,
}
// Erase the host's type as well as retaining it weakly. A cached prefix keeps
// only this counter port, never a configuration, kernel or retirement owner.
trait AssociationMetrics: Send + Sync {
    fn created(&self);
    fn closed(&self, kind: UdpAssociationCloseKind);
}
impl AssociationMetrics for crate::runtime::current_services::CurrentServiceSource {
    fn created(&self) {
        self.engine().record_udp_upstream_association_created();
    }
    fn closed(&self, kind: UdpAssociationCloseKind) {
        match kind {
            UdpAssociationCloseKind::Closed => {
                self.engine().record_udp_upstream_association_closed()
            }
            UdpAssociationCloseKind::IdleTimeout => {
                self.engine().record_udp_upstream_association_idle_timeout()
            }
            UdpAssociationCloseKind::Dropped => {
                self.engine().record_udp_upstream_association_dropped()
            }
        }
    }
}
impl UdpAssociationObserver {
    pub(crate) fn observe_control(
        &self,
        actual: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
        reason: std::sync::Arc<std::sync::atomic::AtomicU8>,
    ) -> zero_platform_tokio::contract::lifecycle::BoxChannelControl {
        zero_platform_tokio::contract::lifecycle::erase_control(control::ObservedControl::new(
            actual,
            self.clone(),
            reason,
        ))
    }
    pub(crate) fn from_execution(
        execution: &crate::protocol_registry::TcpExecutionServices,
    ) -> Self {
        Self {
            host: execution.current_source.clone(),
        }
    }
    pub(crate) fn record_created(&self) {
        if let Some(host) = self.host.upgrade() {
            host.created();
        }
    }
    pub(crate) fn record_close(&self, kind: UdpAssociationCloseKind) {
        let Some(host) = self.host.upgrade() else {
            return;
        };
        host.closed(kind);
    }
}
impl UdpRuntimeServices {
    pub(crate) fn resume_flow(
        mut self,
        session: &crate::runtime::udp_flow::sessions::UdpFlowSession,
    ) -> Result<Self, zero_engine::EngineError> {
        if let Some(original) = session.invocation() {
            self.tcp = self.tcp.resume_business_identity(session, original)?;
        }
        Ok(self)
    }
    pub(crate) fn association_observer(&self) -> UdpAssociationObserver {
        UdpAssociationObserver {
            host: self.tcp.execution().current_source.clone(),
        }
    }
    pub(crate) fn datagram_execution(
        &self,
        inbound_tag: &str,
        session: &zero_core::Session,
    ) -> Result<crate::protocol_registry::TcpExecutionServices, zero_engine::EngineError> {
        Ok(self
            .tcp
            .clone()
            .for_inbound(inbound_tag.to_owned())
            .with_business_identity(session)?
            .execution())
    }
}

#[cfg(test)]
mod tests;
