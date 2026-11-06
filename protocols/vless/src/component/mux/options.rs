use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
use zero_transport::{pooled_driver::PoolOptions, RuntimeError};

pub struct PreparedMux {
    pub(super) leaf: crate::transport::VlessOutboundLeaf,
    pub(super) pool: Arc<PoolOptions>,
    pub(super) material: Option<Arc<super::super::Options>>,
}
impl PreparedMux {
    pub fn from_options_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        input: crate::transport::VlessOutboundBuildOptionsRef<
            '_,
            TTls,
            TWs,
            TGrpc,
            TH2,
            THttp,
            TSplit,
        >,
        runtime: &crate::transport::VlessTransportRuntime,
        source_dir: Option<&Path>,
        binding: UpstreamBinding,
        carriers: NonZeroUsize,
        open_budget: Duration,
    ) -> Result<Self, RuntimeError>
    where
        TTls: zero_traits::ClientTlsProfile + ?Sized,
        TWs: zero_traits::WebSocketTransportProfile + ?Sized,
        TGrpc: zero_traits::GrpcTransportProfile + ?Sized,
        TH2: zero_traits::H2TransportProfile + ?Sized,
        THttp: zero_traits::HttpUpgradeTransportProfile + ?Sized,
        TSplit: zero_traits::SplitHttpTransportProfile + ?Sized,
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
    pub fn from_owned_options_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        input: crate::transport::VlessOutboundBuildOptionsRef<
            '_,
            TTls,
            TWs,
            TGrpc,
            TH2,
            THttp,
            TSplit,
        >,
        runtime: &crate::transport::VlessTransportRuntime,
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
        TH2: zero_traits::H2TransportProfile + ?Sized,
        THttp: zero_traits::HttpUpgradeTransportProfile + ?Sized,
        TSplit: zero_traits::SplitHttpTransportProfile + ?Sized,
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
    fn prepare<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        input: crate::transport::VlessOutboundBuildOptionsRef<
            '_,
            TTls,
            TWs,
            TGrpc,
            TH2,
            THttp,
            TSplit,
        >,
        runtime: &crate::transport::VlessTransportRuntime,
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
        TH2: zero_traits::H2TransportProfile + ?Sized,
        THttp: zero_traits::HttpUpgradeTransportProfile + ?Sized,
        TSplit: zero_traits::SplitHttpTransportProfile + ?Sized,
    {
        if input
            .protocol
            .mux_concurrency
            .is_none_or(|limit| limit == 0)
        {
            return Err(zero_core::Error::Config("native MUX requires enabled concurrency").into());
        }
        if input.protocol.flow.is_some() || input.protocol.reality.is_some() {
            return Err(zero_core::Error::Unsupported(
                "native MUX flow/REALITY requires its complete owner",
            )
            .into());
        }
        // The full material remains with this actual MUX owner, or with its
        // separately declared cached gRPC donor. Bare library construction
        // keeps its stricter original resource policy.
        let mut ordinary = input.clone();
        ordinary.protocol.mux_concurrency = None;
        if input
            .protocol
            .xudp_concurrency
            .is_some_and(|n| Some(n) != input.protocol.mux_concurrency)
        {
            return Err(zero_core::Error::Unsupported(
                "native XUDP must share the actual MUX concurrency",
            )
            .into());
        }
        ordinary.protocol.xudp_concurrency = None;
        let material = if let Some(max) = max_file_bytes.filter(|_| input.grpc.is_none()) {
            Some(Arc::new(
                super::super::Options::from_owned_options_refs(ordinary, binding.dial.clone())?
                    .with_owned_material(runtime, source_dir, max)?,
            ))
        } else {
            if input.grpc.is_some() && max_file_bytes.is_some() {
                // The actual lower cached gRPC owner retains TLS/File/ECH.
                super::super::Options::from_options_refs_inner(
                    ordinary,
                    binding.dial.clone(),
                    true,
                )?;
            } else {
                super::super::Options::from_options_refs(ordinary, binding.dial.clone())?;
            }
            None
        };
        let leaf =
            crate::transport::VlessOutboundLeaf::from_options_refs(source_dir, input, runtime)?;
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
    /// The same runtime gRPC cache lends the already layered carrier. The MUX
    /// wire still runs in this pool's one original driver, without another H2.
    pub fn with_grpc_lender(mut self, dependency: ComponentId) -> Self {
        self.pool = Arc::new((*self.pool).clone().with_stream_dependency(dependency));
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
