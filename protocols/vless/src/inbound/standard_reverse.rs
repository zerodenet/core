//! Reverse control carries no invented business session or detached driver.
use super::*;
use zero_platform_tokio::contract::{
    context::HandshakeContext, ChannelError, Impact, Reason, Stage,
};
impl VlessInboundProfile {
    pub(crate) fn standard_reverse<S: zero_platform_tokio::ClientStream + 'static>(
        &self,
        stream: S,
        request: AuthenticatedRequest,
        backlog: crate::mux::MuxResponseBacklogPolicy,
        context: &HandshakeContext,
    ) -> Result<Box<dyn zero_core::inbound::InboundControlSession>, ChannelError> {
        let caller = context
            .accepted
            .clone()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let tag = request.reverse_tag.ok_or_else(|| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Session)
        })?;
        Ok(Box::new(Control {
            stream,
            uuid: request.request.id,
            flow: request.request.flow,
            testseed: request.testseed,
            auth: request.auth,
            portal: self.portals.portal(&tag),
            backlog,
            caller,
        }))
    }
}
struct Control<S> {
    stream: S,
    uuid: [u8; 16],
    flow: Option<&'static str>,
    testseed: [u32; 4],
    auth: SessionAuth,
    portal: crate::reverse::Portal,
    backlog: crate::mux::MuxResponseBacklogPolicy,
    caller: zero_platform_tokio::AcceptedCaller,
}
impl<S: zero_platform_tokio::ClientStream + 'static> zero_core::inbound::InboundControlSession
    for Control<S>
{
    fn auth(&self) -> Option<&SessionAuth> {
        Some(&self.auth)
    }
    fn run(
        self: Box<Self>,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async move {
            let Self {
                mut stream,
                uuid,
                flow,
                testseed,
                portal,
                backlog,
                caller,
                ..
            } = *self;
            zero_traits::AsyncSocket::write_all(&mut stream, &[crate::shared::VLESS_VERSION, 0])
                .await
                .map_err(|_| Error::Io("Rvs response write failed"))?;
            let stream = if crate::flow::is_vision_flow(flow) {
                VlessInboundTcpStream::vision_after_response(stream, uuid, testseed)
            } else {
                VlessInboundTcpStream::plain(stream)
            };
            portal.attach_scoped(stream, backlog, &caller)?.run().await
        })
    }
}
