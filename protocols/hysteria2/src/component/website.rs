//! Website continuations never allocate a proxy session or route business data.
use super::*;
use zero_platform_tokio::{
    contract::{context::HandshakeContext, ChannelError},
    AcceptedCaller, TokioSocket,
};
pub(super) struct Incoming {
    pub socket: TokioSocket,
    pub site: crate::transport::Hysteria2Website,
    pub resources: Arc<dyn ResourceAccess>,
}
impl InboundHandshake for Incoming {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let caller = context
            .accepted
            .ok_or_else(|| connection::error("website accepted caller missing"))?;
        Ok(AcceptedRequest::Control(Box::new(Control {
            incoming: self,
            caller,
        })))
    }
}
struct Control {
    incoming: Incoming,
    caller: AcceptedCaller,
}
impl zero_core::inbound::InboundControlSession for Control {
    fn auth(&self) -> Option<&zero_core::SessionAuth> {
        None
    }
    fn run(
        self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), zero_core::Error>> + Send>>
    {
        Box::pin(async move {
            let peer = self
                .incoming
                .socket
                .peer_addr()
                .map_err(|_| zero_core::Error::Io("website peer"))?;
            self.incoming
                .site
                .serve_scoped(
                    self.incoming.socket,
                    peer,
                    self.incoming.resources,
                    self.caller,
                )
                .await
                .map_err(|source| {
                    tracing::debug!(%source,"website connection ended");
                    zero_core::Error::Io("website connection")
                })
        })
    }
}
