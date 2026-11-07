//! Standard roles borrow the same scoped stack; no second users/device pool.
use super::*;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::{
    context::OpenContext,
    datagram::{BoxDatagramSession, DatagramRequest},
    scope::{TaskJoinWitness, TaskScopeIdentity},
    stream::{Connected, PeerConfirmation},
    ChannelError, Impact, Reason, Stage,
};
pub(crate) struct PacketFlowBorrow {
    pub(crate) caller: TaskJoinWitness,
    pub(crate) custody: Option<zero_platform_tokio::contract::component::PacketDriverCallRetained>,
    pub(crate) observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl SharedRawIpDevice {
    pub(crate) fn open_standard_packet_user(
        &self,
        scope: &TaskScopeIdentity,
        caller: TaskJoinWitness,
    ) -> Result<
        (
            crate::runtime::packet_flow::FlowUser,
            crate::runtime::packet_flow::PacketSourceClose,
        ),
        ChannelError,
    > {
        self.check_standard_scope(scope)?;
        let users = self
            .task
            .flow_users()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Packet))?;
        users.acquire_packet_source(caller)
    }

    pub(crate) fn open_standard_datagram(
        &self,
        scope: &TaskScopeIdentity,
        local: IpAddr,
        peer: SocketAddr,
        request: DatagramRequest,
        context: OpenContext,
        borrowed: PacketFlowBorrow,
    ) -> Result<
        (
            BoxDatagramSession,
            crate::runtime::packet_flow::FlowReceiverBinding,
        ),
        ChannelError,
    > {
        self.check_standard_scope(scope)?;
        let DatagramTargets::Fixed(target) = request.targets else {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        };
        let users = self
            .task
            .flow_users()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let user = users.acquire_from_packet_call(borrowed.caller, borrowed.custody)?;
        let receiver = user.receiver_binding();
        let socket = match self.udp.bind_observed(local, borrowed.observer) {
            Ok(socket) => socket,
            Err(source) => {
                // This synchronous virtual bind rejected before returning any
                // tuple/socket. The same per-attempt token has no virtual IO.
                user.mark_logical_closed();
                return Err(
                    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session)
                        .with_source(super::StackConstructionError::Udp(source)),
                );
            }
        };
        // Same legacy users table: the real caller join and each transferred
        // original view remain separate from the role and native-driver fences.
        let session =
            zero_platform_tokio::contract::datagram::erase_session(super::session_udp::session(
                socket,
                peer,
                target,
                request.association,
                context.cancel,
                user,
                users.cancellation().clone(),
            ));
        Ok((session, receiver))
    }
    pub(crate) fn open_standard_stream(
        &self,
        scope: &TaskScopeIdentity,
        local: IpAddr,
        peer: SocketAddr,
        context: OpenContext,
        borrowed: PacketFlowBorrow,
    ) -> Result<Connected<super::session_tcp::Stream>, ChannelError> {
        self.check_standard_scope(scope)?;
        let users = self
            .task
            .flow_users()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let user = users.acquire_from_packet_call(borrowed.caller, borrowed.custody)?;
        let opening = self.tcp.prepare_connect(local, peer, borrowed.observer);
        Ok(Connected {
            io: super::session_tcp::Stream::new(
                opening,
                context,
                user,
                users.cancellation().clone(),
            ),
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
    pub(super) fn check_standard_scope(
        &self,
        scope: &TaskScopeIdentity,
    ) -> Result<(), ChannelError> {
        if self.is_usable() && self.task.matches_scope(scope) {
            Ok(())
        } else {
            Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ))
        }
    }
}
