use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
use zero_transport::{pooled_driver::PoolOptions, RuntimeError};

pub struct PreparedMux {
    pub(super) leaf: crate::transport::VmessOutboundLeaf,
    pub(super) pool: Arc<PoolOptions>,
}
impl PreparedMux {
    pub fn from_options_refs<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        runtime: &crate::transport::VmessTransportRuntime,
        source_dir: Option<&Path>,
        binding: UpstreamBinding,
        carriers: NonZeroUsize,
        open_budget: Duration,
    ) -> Result<Self, RuntimeError>
    where
        TTls: zero_traits::ClientTlsProfile + ?Sized,
        TWs: zero_traits::WebSocketTransportProfile + ?Sized,
        TGrpc: zero_traits::GrpcTransportProfile + ?Sized,
    {
        if input
            .protocol
            .mux_concurrency
            .is_none_or(|limit| limit == 0)
        {
            return Err(zero_core::Error::Config("native MUX requires enabled concurrency").into());
        }
        // The ordinary protocol-owned validator still rejects resources whose
        // complete owners are absent: CA files, gRPC, browser dialers, etc.
        let mut ordinary = input;
        ordinary.protocol.mux_concurrency = None;
        super::super::Options::from_options_refs(ordinary, binding.dial.clone())?;
        let leaf =
            crate::transport::VmessOutboundLeaf::from_options_refs(source_dir, input, runtime)?;
        let pool = Arc::new(leaf.native_mux_options(binding, carriers, open_budget)?);
        Ok(Self { leaf, pool })
    }
    pub fn pool_options(&self) -> &Arc<PoolOptions> {
        &self.pool
    }
    pub fn bind(self, pool_owner: ComponentId) -> StreamOptions {
        let mut identity = super::PoolFactory
            .describe(&self.pool)
            .static_identity
            .to_vec();
        identity.extend_from_slice(&pool_owner.0.to_le_bytes());
        StreamOptions {
            prepared: self,
            pool_owner,
            identity: identity.into(),
        }
    }
}
pub struct StreamOptions {
    pub(super) prepared: PreparedMux,
    pub(super) pool_owner: ComponentId,
    pub(super) identity: Arc<[u8]>,
}
