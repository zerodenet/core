use crate::{
    component::Options,
    transport::{VlessOutboundBuildOptionsRef, VlessOutboundLeaf, VlessTransportRuntime},
};
use std::{num::NonZeroUsize, path::Path, sync::Arc};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::component::ComponentFactory;
use zero_traits::{
    ClientTlsProfile, DialPolicy, GrpcTransportProfile, H2TransportProfile,
    HttpUpgradeTransportProfile, SplitHttpTransportProfile, WebSocketTransportProfile,
};
use zero_transport::{grpc::NativePoolOptions, RuntimeError};
#[derive(Debug)]
pub struct PreparedGrpc {
    pub(super) protocol: Options,
    pub(super) pool: NativePoolOptions,
}
impl PreparedGrpc {
    pub fn from_options_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        input: VlessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc, TH2, THttp, TSplit>,
        runtime: &VlessTransportRuntime,
        source_dir: Option<&Path>,
        dial: DialPolicy,
        carriers: NonZeroUsize,
        streams: NonZeroUsize,
    ) -> Result<Self, RuntimeError>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: H2TransportProfile + ?Sized,
        THttp: HttpUpgradeTransportProfile + ?Sized,
        TSplit: SplitHttpTransportProfile + ?Sized,
    {
        if input.grpc.is_none() || input.protocol.flow.is_some() || input.protocol.reality.is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "managed gRPC requires ordinary VLESS without flow/REALITY",
            )
            .into());
        }
        // Shared parsing keeps all existing protocol validation. Bare Options
        // still rejects gRPC; only this explicitly resource-bound recipe admits it.
        let protocol = Options::from_options_refs_inner(input.clone(), dial.clone(), true)
            .map_err(|source| std::io::Error::new(std::io::ErrorKind::InvalidInput, source))?;
        let leaf = VlessOutboundLeaf::from_options_refs(source_dir, input, runtime)
            .map_err(|source| std::io::Error::new(std::io::ErrorKind::InvalidInput, source))?;
        let pool = leaf.native_grpc_options(runtime, dial, carriers, streams)?;
        Ok(Self { protocol, pool })
    }
    pub fn pool_options(&self) -> &NativePoolOptions {
        &self.pool
    }
    pub fn bind(self, pool_owner: ComponentId) -> StreamOptions {
        let mut identity = self.protocol.identity.to_vec();
        identity.extend_from_slice(&super::PoolFactory.describe(&self.pool).static_identity);
        identity.extend_from_slice(&pool_owner.0.to_le_bytes());
        StreamOptions {
            prepared: self,
            pool_owner,
            identity: identity.into(),
        }
    }
}
#[derive(Debug)]
pub struct StreamOptions {
    pub(super) prepared: PreparedGrpc,
    pub(super) pool_owner: ComponentId,
    pub(super) identity: Arc<[u8]>,
}
