//! The actual accepted entry crosses source erasure without losing its Scope.
use super::super::{context::InboundConnectionFacts, lifecycle::TaskClass, scope::TaskJoinWitness};
use super::*;
use crate::{AcceptedTasks, AcceptedTcpClaim, AcceptedTcpTaskReceiver};
use zero_core::component::TaskId;

pub(crate) struct TcpAdmission {
    pub(crate) origin: crate::controlled_listener::accepted::graph::PendingTcp,
    pub(crate) facts: InboundConnectionFacts,
}

impl PendingInbound {
    /// Facts of a native multiplexed carrier. Native users/close authority stays
    /// with its declared provider; this grants no TCP claim or task authority.
    pub fn with_carrier_facts(mut self, facts: InboundConnectionFacts) -> Self {
        self.carrier = Some(facts);
        self
    }
    pub fn carrier_facts(&self) -> Option<&InboundConnectionFacts> {
        self.carrier.as_ref()
    }

    pub(crate) fn with_tcp_admission(mut self, admission: TcpAdmission) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Captured by the original native accept, before protocol preparation.
    pub fn connection_facts(&self) -> Option<&InboundConnectionFacts> {
        self.admission
            .as_ref()
            .map(|admission| &admission.facts)
            .or_else(|| self.peer.as_ref().map(|peer| peer.facts()))
    }

    /// Weak access to the original entry, never another close owner or Scope.
    pub fn accepted_tcp(&self) -> Option<(AcceptedTcpClaim, AcceptedTcpTaskReceiver)> {
        self.admission
            .as_ref()
            .map(|admission| admission.origin.receipts())
    }

    /// Publish the real primary task before polling any protocol handshake.
    /// Only a pending request produced from an actual accepted entry qualifies.
    pub fn spawn<F, Fut>(
        mut self,
        class: TaskClass,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(Self, AcceptedTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let admission = self.admission.take().ok_or_else(|| {
            ChannelError::new(
                super::super::Stage::Prepare,
                super::super::Reason::Unsupported,
                super::super::Impact::Component,
            )
        })?;
        admission
            .origin
            .spawn(class, move |tasks| make(self, tasks))
    }
}

impl PendingInbound {
    pub fn datagram_peer(&self) -> Option<&super::udp::DatagramPeerAdmission> {
        self.peer.as_ref()
    }
}
