//! Standard roles borrow the same scoped stack; no second users/device pool.
use super::*;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::{
    context::OpenContext,
    datagram::{BoxDatagramSession, DatagramRequest},
    scope::TaskScopeIdentity,
    stream::{Connected, PeerConfirmation},
    CancellationToken, ChannelError, Impact, Reason, Stage,
};
impl SharedRawIpDevice {
    pub(crate) fn open_standard_datagram(
        &self,
        scope: &TaskScopeIdentity,
        local: IpAddr,
        peer: SocketAddr,
        request: DatagramRequest,
        cancel: CancellationToken,
    ) -> Result<BoxDatagramSession, ChannelError> {
        self.check_standard_scope(scope)?;
        let DatagramTargets::Fixed(target) = request.targets else {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        };
        let socket = self.udp.bind_observed(local, None).map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session)
                .with_source(super::StackConstructionError::Udp(source))
        })?;
        // This actual slot's standard fence accounts the transferred view and
        // caller. Old untracked leaf APIs keep their independent FlowUsers lane.
        Ok(zero_platform_tokio::contract::datagram::erase_session(
            super::session_udp::session(socket, peer, target, request.association, cancel),
        ))
    }
    pub(crate) fn open_standard_stream(
        &self,
        scope: &TaskScopeIdentity,
        local: IpAddr,
        peer: SocketAddr,
        context: OpenContext,
    ) -> Result<Connected<super::session_tcp::Stream>, ChannelError> {
        self.check_standard_scope(scope)?;
        let opening = self.tcp.prepare_connect(local, peer, None);
        Ok(Connected {
            io: super::session_tcp::Stream::new(opening, context),
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
