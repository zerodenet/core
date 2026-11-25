use super::Options;
use std::sync::Arc;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    context::OpenContext,
    datagram::DatagramRequest,
    lifecycle::BuildError,
};
use zero_traits::DialPolicy;
use zero_transport::{
    pooled_driver::{datagram::PooledDatagram, PooledStream},
    runner::{
        HandshakeError, PooledBoundLeaf, PooledDatagramProtocol, PooledRoleFactory,
        PooledRoleOptions, PooledRoleProtocol,
    },
    RuntimeError,
};

impl PooledRoleOptions for Options {
    fn pool_owner(&self) -> ComponentId {
        self.pool_owner
    }

    fn dial(&self) -> &DialPolicy {
        &self.prepared.pool.binding().dial
    }

    fn static_identity(&self) -> Arc<[u8]> {
        self.identity.clone()
    }

    fn supports_datagram(&self) -> bool {
        self.datagram
    }
}

impl PooledBoundLeaf for crate::transport::TrojanOutboundLeaf {
    fn pool_instance(&self) -> Option<ComponentInstanceId> {
        self.native_mux_instance().ok()
    }

    async fn open_stream(
        &self,
        destination: Destination,
        context: &OpenContext,
    ) -> Result<PooledStream, RuntimeError> {
        self.open_native_mux(destination, context).await
    }
}

#[derive(Clone, Copy, Default)]
struct TrojanMuxRole;

impl PooledRoleProtocol for TrojanMuxRole {
    type Options = Options;
    type BoundLeaf = crate::transport::TrojanOutboundLeaf;

    fn bind_leaf(
        &self,
        options: &Options,
        pool: ComponentInstanceId,
    ) -> Result<Self::BoundLeaf, RuntimeError> {
        options.prepared.leaf.bind_native_mux(pool)
    }
}

impl PooledDatagramProtocol<TrojanMuxRole> for TrojanMuxRole {
    type PacketIo = crate::mux::native::DatagramIo;

    fn validate_request(&self, request: &DatagramRequest) -> Result<(), HandshakeError> {
        if !matches!(&request.targets, DatagramTargets::Fixed(_)) {
            return Err(HandshakeError::unsupported());
        }
        Ok(())
    }

    async fn open_datagram(
        &self,
        leaf: &crate::transport::TrojanOutboundLeaf,
        _request: &DatagramRequest,
        context: &OpenContext,
    ) -> Result<PooledDatagram<Self::PacketIo>, RuntimeError> {
        leaf.open_native_mux_udp(context).await
    }
}

pub struct Factory;

impl ComponentFactory for Factory {
    type Options = Options;

    fn describe(&self, options: &Options) -> ComponentSpec {
        PooledRoleFactory::with_datagram(TrojanMuxRole, TrojanMuxRole).describe(options)
    }

    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        PooledRoleFactory::with_datagram(TrojanMuxRole, TrojanMuxRole).build(options, context)
    }
}
