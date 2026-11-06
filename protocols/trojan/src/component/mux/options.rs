use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
use zero_transport::{pooled_driver::PoolOptions, RuntimeError};

pub struct PreparedMux {
    pub(super) leaf: crate::transport::TrojanOutboundLeaf,
    pub(super) pool: Arc<PoolOptions>,
}
impl PreparedMux {
    pub fn from_options_refs<TWs, TGrpc>(
        input: crate::transport::TrojanOutboundBuildOptionsRef<'_, TWs, TGrpc>,
        runtime: &crate::mux::TrojanMuxConnectionPool,
        source_dir: Option<&Path>,
        binding: UpstreamBinding,
        carriers: NonZeroUsize,
        open_budget: Duration,
    ) -> Result<Self, RuntimeError>
    where
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
        let leaf = crate::transport::TrojanOutboundLeaf::from_options_refs(
            source_dir,
            input,
            runtime.clone(),
        )?;
        let pool = Arc::new(leaf.native_mux_options(binding, carriers, open_budget)?);
        Ok(Self { leaf, pool })
    }
    pub fn with_node_dependency(mut self, dependency: ComponentId) -> Self {
        let pool = (*self.pool).clone().with_node_dependency(dependency);
        self.pool = Arc::new(pool);
        self
    }
    pub fn pool_options(&self) -> &Arc<PoolOptions> {
        &self.pool
    }
    pub fn bind(self, pool_owner: ComponentId) -> Options {
        let mut identity = super::PoolFactory
            .describe(&self.pool)
            .static_identity
            .to_vec();
        identity.extend_from_slice(&pool_owner.0.to_le_bytes());
        Options {
            prepared: self,
            pool_owner,
            datagram: true,
            identity: identity.into(),
        }
    }
}
pub struct Options {
    pub(super) prepared: PreparedMux,
    pub(super) pool_owner: ComponentId,
    pub(super) datagram: bool,
    pub(super) identity: Arc<[u8]>,
}

impl Options {
    /// The protocol factory declares and constructs only its qualified Stream.
    pub fn stream_only(mut self) -> Self {
        if self.datagram {
            self.datagram = false;
            let mut identity = self.identity.to_vec();
            identity.push(0);
            self.identity = identity.into();
        }
        self
    }
}
