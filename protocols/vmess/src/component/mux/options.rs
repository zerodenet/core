use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
use zero_transport::{pooled_driver::PoolOptions, RuntimeError};

pub struct PreparedMux {
    pub(super) leaf: crate::transport::VmessOutboundLeaf,
    pub(super) pool: Arc<PoolOptions>,
    pub(super) material: Option<Arc<super::super::Options>>,
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
        Self::prepare(
            input,
            runtime,
            source_dir,
            binding,
            carriers,
            open_budget,
            None,
        )
    }
    pub fn from_owned_options_refs<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        runtime: &crate::transport::VmessTransportRuntime,
        source_dir: Option<&Path>,
        binding: UpstreamBinding,
        carriers: NonZeroUsize,
        open_budget: Duration,
        max_file_bytes: NonZeroUsize,
    ) -> Result<Self, RuntimeError>
    where
        TTls: zero_traits::ClientTlsProfile + ?Sized,
        TWs: zero_traits::WebSocketTransportProfile + ?Sized,
        TGrpc: zero_traits::GrpcTransportProfile + ?Sized,
    {
        Self::prepare(
            input,
            runtime,
            source_dir,
            binding,
            carriers,
            open_budget,
            Some(max_file_bytes),
        )
    }
    fn prepare<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        runtime: &crate::transport::VmessTransportRuntime,
        source_dir: Option<&Path>,
        binding: UpstreamBinding,
        carriers: NonZeroUsize,
        open_budget: Duration,
        max_file_bytes: Option<NonZeroUsize>,
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
        // The owning recipe retains the same full TLS/File profile. Fixed
        // library construction keeps its original stricter resource policy;
        // gRPC/browser still require their complete transport owners.
        let mut ordinary = input;
        ordinary.protocol.mux_concurrency = None;
        let material = match max_file_bytes {
            Some(max) => Some(Arc::new(
                super::super::Options::from_owned_options_refs_at(
                    ordinary,
                    binding.dial.clone(),
                    source_dir,
                    Some(max),
                )?
                .with_owned_material(runtime, source_dir, max)?,
            )),
            None => {
                super::super::Options::from_options_refs(ordinary, binding.dial.clone())?;
                None
            }
        };
        let leaf =
            crate::transport::VmessOutboundLeaf::from_options_refs(source_dir, input, runtime)?;
        let pool = Arc::new(leaf.native_mux_options(binding, carriers, open_budget)?);
        Ok(Self {
            leaf,
            pool,
            material,
        })
    }
    pub fn with_node_dependency(mut self, dependency: ComponentId) -> Self {
        let pool = (*self.pool).clone().with_node_dependency(dependency);
        self.pool = Arc::new(pool);
        self
    }
    pub fn reuse_file_material_from(
        &mut self,
        previous: zero_core::component::ComponentInstanceId,
    ) -> Result<bool, RuntimeError> {
        match self.material.as_mut().and_then(Arc::get_mut) {
            Some(options) => options.reuse_file_material_from(previous),
            None => Ok(false),
        }
    }
    pub fn remember_cold_material_identity(&self, identity: &[u8]) -> std::io::Result<()> {
        match &self.material {
            Some(options) => options.remember_cold_material_identity(identity),
            None => Ok(()),
        }
    }
    pub fn reuse_cold_file_material_from(&mut self, identity: &[u8]) -> std::io::Result<bool> {
        match self.material.as_mut().and_then(Arc::get_mut) {
            Some(options) => options.reuse_cold_file_material_from(identity),
            None => Ok(false),
        }
    }
    pub fn owned_pool_options(&self) -> Arc<super::OwnedPoolOptions> {
        Arc::new(super::OwnedPoolOptions {
            pool: self.pool.clone(),
            material: self.material.clone(),
        })
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
    /// Qualify only the Stream executor without advertising the packet role.
    /// The protocol factory owns both its description and actual role handles.
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
