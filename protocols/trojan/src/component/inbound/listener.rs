//! Trojan standard TCP/TLS inbound source backed by [`StandardTcpInboundFactory`].

use crate::{inbound::TrojanInboundProfileUpdate, transport::TrojanInboundListenerRequest};
use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess},
    context::HandshakeContext,
    inbound::AcceptedRequest,
    lifecycle::BuildError,
    BoxFuture, ChannelError,
};
use zero_transport::runner::{
    StandardTcpInboundFactory, StandardTcpInboundOptions, StandardTcpInboundProtocol,
};

pub struct TrojanStandardInbound;

pub type Options = StandardTcpInboundOptions<TrojanStandardInbound>;

impl StandardTcpInboundProtocol for TrojanStandardInbound {
    type Staged = (TrojanInboundListenerRequest, TrojanInboundProfileUpdate);
    type Active = TrojanInboundListenerRequest;

    fn tls_material(
        staged: &Self::Staged,
    ) -> Vec<Arc<zero_transport::tls::source::PreparedServer>> {
        vec![staged.0.tls_material()]
    }

    fn activate((request, update): Self::Staged) -> Result<Self::Active, ChannelError> {
        Ok(request.with_profile(update.commit()))
    }

    fn accept(
        active: Self::Active,
        socket: zero_platform_tokio::TokioSocket,
        _resources: Arc<dyn ResourceAccess>,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>> {
        Box::pin(active.accept_standard_request(socket, context))
    }
}

pub struct Factory;

impl ComponentFactory for Factory {
    type Options = Options;

    fn describe(&self, options: &Options) -> ComponentSpec {
        StandardTcpInboundFactory::<TrojanStandardInbound>::new().describe(options)
    }

    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        StandardTcpInboundFactory::<TrojanStandardInbound>::new().build(options, context)
    }
}
