use super::{UdpAssociationCloseKind, UdpRuntimeServices};
/// Actual completion statistics only. No network, registry or retirement owner
/// is retained by a controller living inside RetiredDatagrams.
#[derive(Clone)]
pub(crate) struct UdpAssociationObserver {
    host: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
}
impl UdpAssociationObserver {
    pub(crate) fn record_close(&self, kind: UdpAssociationCloseKind) {
        let Some(host) = self.host.upgrade() else {
            return;
        };
        match kind {
            UdpAssociationCloseKind::Closed => {
                host.engine().record_udp_upstream_association_closed()
            }
            UdpAssociationCloseKind::IdleTimeout => {
                host.engine().record_udp_upstream_association_idle_timeout()
            }
            UdpAssociationCloseKind::Dropped => {
                host.engine().record_udp_upstream_association_dropped()
            }
        }
    }
}
impl UdpRuntimeServices {
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
