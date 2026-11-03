use super::super::model::{RegisteredUdpConnection, RegisteredUdpState};
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;

impl RegisteredUdpState {
    pub(in crate::runtime) fn register_connection(
        &mut self,
        session_id: Option<u64>,
        connection: RegisteredUdpConnection,
    ) -> ManagedUdpFlowRef {
        if let Some(session_id) = session_id {
            self.release_session(session_id);
        }
        let flow_ref = ManagedUdpFlowRef::new(self.next_managed_flow_id);
        self.next_managed_flow_id += 1;
        self.connections.insert(flow_ref, connection);
        if let Some(session_id) = session_id {
            self.sessions.insert(session_id, flow_ref);
        }
        flow_ref
    }

    pub(crate) fn release_session(&mut self, session_id: u64) {
        #[cfg(feature = "managed-stream-runtime")]
        self.logical_sessions.remove(&session_id);
        if let Some(id) = self.sessions.remove(&session_id) {
            self.connections.remove(&id);
        }
    }
}

#[cfg(feature = "upstream-association-runtime")]
impl RegisteredUdpState {
    pub(crate) fn register_upstream_flow(
        &mut self,
        target: super::super::super::upstream::DatagramAssociationTarget,
    ) -> ManagedUdpFlowRef {
        self.register_connection(None, RegisteredUdpConnection::Upstream(target))
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
