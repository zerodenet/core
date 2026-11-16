use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
use zero_transport::{pooled_driver::PoolOptions, RuntimeError};

pub struct PreparedMux {
    pub(super) leaf: crate::transport::VlessOutboundLeaf,
    pub(super) pool: Arc<PoolOptions>,
    pub(super) material: Option<Arc<super::super::Options>>,
    stream: bool,
    datagram: bool,
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
        let vision = crate::flow::is_vision_flow(input.protocol.flow);
        if input.protocol.mux_concurrency.is_none()
            && input.protocol.xudp_concurrency.is_none()
            && !vision
            || input.protocol.mux_concurrency == Some(0)
            || input.protocol.xudp_concurrency == Some(0)
        {
            return Err(zero_core::Error::Config("native MUX requires enabled concurrency").into());
        }
        if vision && input.protocol.mux_concurrency.is_some()
            || (input.protocol.reality.is_some() && max_file_bytes.is_none())
        {
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
        ordinary.protocol.xudp_concurrency = None;
        let preconnect = input.protocol.testpre != 0;
        let packet = input.mkcp.is_some()
            || input.protocol.quic.is_some()
            || input.split_http.is_some()
            || input.ws.is_some_and(|ws| {
                zero_traits::WebSocketTransportProfile::browser_dialer(ws).is_some()
            });
        if packet {
            ordinary.mkcp = None;
            ordinary.hysteria = None;
            ordinary.protocol.quic = None;
            ordinary.final_mask = None;
            ordinary.split_http = None;
            if ordinary.ws.is_some_and(|ws| {
                zero_traits::WebSocketTransportProfile::browser_dialer(ws).is_some()
            }) {
                ordinary.ws = None;
            }
        }
        ordinary.protocol.testpre = 0;
        let material = if let Some(max) =
            max_file_bytes.filter(|_| input.grpc.is_none() && !preconnect && !packet)
        {
            Some(Arc::new(
                super::super::Options::from_owned_options_refs(ordinary, binding.dial.clone())?
                    .with_owned_material(runtime, source_dir, max)?,
            ))
        } else {
            if (input.grpc.is_some() || preconnect || packet) && max_file_bytes.is_some() {
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
        let prepared_reality = material
            .as_ref()
            .and_then(|options| options.reality.clone());
        let tcp = input.protocol.mux_concurrency;
        // Vision's existing UDP plan always uses XUDP, including its implicit
        // default. Ordinary TCP keeps the same flow and bypass-capable original.
        let udp = if vision {
            Some(input.protocol.xudp_concurrency.unwrap_or(1))
        } else {
            input.protocol.xudp_concurrency
        };
        let leaf = if max_file_bytes.is_some() {
            // No legacy mask tables are materialized by cold Host selection.
            crate::transport::VlessOutboundLeaf::from_owned_options_refs(
                source_dir, input, runtime,
            )?
            .0
        } else {
            crate::transport::VlessOutboundLeaf::from_options_refs(source_dir, input, runtime)?
        }
        .with_native_mux_policy(tcp, udp)
        .with_preparation_reality(prepared_reality.clone());
        let mut pool = leaf.native_mux_options(binding, carriers, open_budget)?;
        if prepared_reality.is_some() {
            // One setup or rejected-original job in this actual physical slot.
            // Authentication rejection never yields a usable MuxClient/cache.
            pool = pool.with_preparation_jobs();
        }
        let pool = Arc::new(pool);
        Ok(Self {
            leaf,
            pool,
            material,
            stream: tcp.is_some(),
            datagram: udp.is_some(),
        })
    }
    pub fn with_node_dependency(mut self, dependency: ComponentId) -> Self {
        let pool = (*self.pool).clone().with_node_dependency(dependency);
        self.pool = Arc::new(pool);
        self
    }
    /// The same runtime gRPC cache lends the already layered carrier. The MUX
    /// wire still runs in this pool's one original driver, without another H2.
    pub fn with_carrier_lender(mut self, dependency: ComponentId) -> Self {
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
        let stream = self.stream;
        let datagram = self.datagram;
        Options {
            prepared: self,
            pool_owner,
            stream,
            datagram,
            ordinary: None,
            identity: identity.into(),
        }
    }
}
pub struct Options {
    pub(super) prepared: PreparedMux,
    pub(super) pool_owner: ComponentId,
    pub(super) stream: bool,
    pub(super) datagram: bool,
    pub(super) ordinary: Option<OrdinaryPlan>,
    pub(super) identity: Arc<[u8]>,
}

pub(super) enum OrdinaryPlan {
    Ordinary(Arc<super::super::Options>),
    Grpc(Arc<super::super::grpc::StreamOptions>),
}
impl OrdinaryPlan {
    pub(super) fn describe(&self) -> zero_platform_tokio::contract::component::ComponentSpec {
        match self {
            Self::Ordinary(options) => super::super::Factory.describe(options),
            Self::Grpc(options) => super::super::grpc::StreamFactory.describe(options),
        }
    }
}

impl Options {
    pub fn with_ordinary(mut self, options: Arc<super::super::Options>) -> Self {
        self.set_ordinary_plan(OrdinaryPlan::Ordinary(options));
        self
    }
    pub fn with_grpc_ordinary(mut self, options: Arc<super::super::grpc::StreamOptions>) -> Self {
        self.set_ordinary_plan(OrdinaryPlan::Grpc(options));
        self
    }
    /// The same actual callable component's material recovery identity.
    pub fn remember_ordinary_material_identity(&self, identity: &[u8]) -> std::io::Result<()> {
        match self.ordinary.as_ref() {
            Some(OrdinaryPlan::Ordinary(options)) => {
                options.remember_cold_material_identity(identity)
            }
            _ => Ok(()),
        }
    }
    fn set_ordinary_plan(&mut self, plan: OrdinaryPlan) {
        let spec = plan.describe();
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"vless.actual.ordinary-role.v1");
        identity.push(u8::from(matches!(&plan, OrdinaryPlan::Grpc(_))));
        identity.extend_from_slice(&(spec.static_identity.len() as u64).to_le_bytes());
        identity.extend_from_slice(&spec.static_identity);
        self.identity = identity.into();
        self.ordinary = Some(plan);
    }

    /// Read the same already-prepared whole leaf, without another material
    /// decode or native/cache lookup. Execution still needs its actual role.
    pub fn packet_path_metadata(&self) -> (zero_core::request::Destination, bool) {
        let server = self.prepared.leaf.server();
        let address = match zero_core::address::parse_ip_literal(server) {
            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            None => zero_core::Address::Domain(server.to_owned()),
        };
        (
            zero_core::request::Destination::new(address, self.prepared.leaf.port()),
            true,
        )
    }
    /// Keep the existing explicit Stream-only library projection.
    pub fn stream_only(mut self) -> Self {
        if self.datagram || self.stream && self.ordinary.is_some() {
            self.datagram = false;
            if self.stream {
                self.ordinary = None;
            }
            let mut identity = self.identity.to_vec();
            identity.push(0);
            self.identity = identity.into();
        }
        self
    }
}
