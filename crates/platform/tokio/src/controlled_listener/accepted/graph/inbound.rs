//! Move the original byte view into a standard handshake without escaping users.
use super::*;
use crate::contract::{
    context::InboundConnectionFacts,
    inbound::{InboundHandshake, PendingInbound, TcpAdmission},
    lifecycle::TaskClass,
};

pub(crate) struct PendingTcp {
    users: Arc<Users>,
    claim: AcceptedTcpClaim,
    receiver: AcceptedTcpTaskReceiver,
}
impl PendingTcp {
    pub(crate) fn receipts(&self) -> (AcceptedTcpClaim, AcceptedTcpTaskReceiver) {
        (self.claim.clone(), self.receiver.clone())
    }
    pub(crate) fn spawn<F, Fut>(
        self,
        class: TaskClass,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(AcceptedTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        self.users.spawn(class, true, make)
    }
}
impl AcceptedTcp {
    /// Pure preparation only: `make` must not read, authenticate or start tasks.
    /// The original owner retains native close; PendingInbound retains the real
    /// same-entry admission until the host publishes its primary task.
    pub fn into_pending<H: InboundHandshake>(
        self,
        make: impl FnOnce(crate::TokioSocket) -> H,
    ) -> Result<PendingInbound, ChannelError> {
        let AcceptedTcp { io, claim } = self;
        let users = claim.entry.users.clone().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Unsupported,
                "accepted task graph was not declared",
            )
        })?;
        let pool = users.pool.upgrade().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Revoked,
                "accepted native owner disappeared",
            )
        })?;
        let socket = crate::TokioSocket::from_accepted(io);
        let facts = InboundConnectionFacts::from_stream(&socket);
        if let Some(local) = facts.local_endpoint {
            let _ = users.local.set(local);
        }
        let origin = PendingTcp {
            users,
            claim,
            receiver: AcceptedTcpTaskReceiver::new(&pool),
        };
        Ok(PendingInbound::new(make(socket)).with_tcp_admission(TcpAdmission { origin, facts }))
    }
}
