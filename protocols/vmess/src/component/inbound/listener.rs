//! VMess standard TCP/TLS inbound source backed by [`StandardTcpInboundFactory`].

use crate::transport::{PreparedVmessInboundListenerRequest, VmessInboundListenerRequest};
use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess},
    context::HandshakeContext,
    inbound::AcceptedRequest,
    lifecycle::BuildError,
    BoxFuture, ChannelError, Impact, Reason, Stage,
};
use zero_transport::runner::{
    StandardTcpInboundFactory, StandardTcpInboundOptions, StandardTcpInboundProtocol,
};

pub struct VmessStandardInbound;

pub type Options = StandardTcpInboundOptions<VmessStandardInbound>;

impl StandardTcpInboundProtocol for VmessStandardInbound {
    type Staged = PreparedVmessInboundListenerRequest;
    type Active = VmessInboundListenerRequest;

    fn tls_material(
        staged: &Self::Staged,
    ) -> Vec<Arc<zero_transport::tls::source::PreparedServer>> {
        staged.tls_material().into_iter().collect()
    }

    fn activate(staged: Self::Staged) -> Result<Self::Active, ChannelError> {
        staged.activate().map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                .with_source(source)
        })
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
        StandardTcpInboundFactory::<VmessStandardInbound>::new().describe(options)
    }

    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        StandardTcpInboundFactory::<VmessStandardInbound>::new().build(options, context)
    }
}
