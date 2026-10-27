use super::super::model::{RegisteredUdpConnection, RegisteredUdpState};
use crate::runtime::udp_flow::managed::ManagedUdpFlowResume;
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;

impl RegisteredUdpState {
    pub(crate) fn register_managed_flow(
        &mut self,
        resume: ManagedUdpFlowResume,
    ) -> ManagedUdpFlowRef {
        self.register_connection(RegisteredUdpConnection::Legacy(resume))
    }

    pub(in crate::runtime::udp_flow::registered) fn register_connection(
        &mut self,
        connection: RegisteredUdpConnection,
    ) -> ManagedUdpFlowRef {
        let flow_ref = ManagedUdpFlowRef::new(self.next_managed_flow_id);
        self.next_managed_flow_id += 1;
        self.connections.insert(flow_ref, connection);
        flow_ref
    }

    #[cfg(test)]
    pub(crate) fn managed_flow_resume(
        &self,
        flow_ref: ManagedUdpFlowRef,
    ) -> Option<ManagedUdpFlowResume> {
        match self.connections.get(&flow_ref)? {
            #[cfg(feature = "managed-stream-runtime")]
            RegisteredUdpConnection::Logical(_) => None,
            #[cfg(feature = "upstream-association-runtime")]
            RegisteredUdpConnection::Upstream(_) => None,
            RegisteredUdpConnection::Legacy(resume) => Some(resume.clone()),
        }
    }
}

#[cfg(feature = "upstream-association-runtime")]
impl RegisteredUdpState {
    pub(crate) fn register_upstream_flow(
        &mut self,
        target: super::super::super::upstream::DatagramAssociationTarget,
    ) -> ManagedUdpFlowRef {
        self.register_connection(RegisteredUdpConnection::Upstream(target))
    }
    pub(crate) fn upstream_flow_target(
        &self,
        flow_ref: ManagedUdpFlowRef,
    ) -> Option<super::super::super::upstream::DatagramAssociationTarget> {
        match self.connections.get(&flow_ref)? {
            RegisteredUdpConnection::Upstream(target) => Some(target.clone()),
            _ => None,
        }
    }
}
