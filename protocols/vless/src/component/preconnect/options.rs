use crate::transport::{VlessOutboundBuildOptionsRef, VlessOutboundLeaf, VlessTransportRuntime};
use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::{pooled_driver::PoolOptions, RuntimeError};

#[derive(Clone)]
pub struct PreparedPreconnect {
    pub(super) protocol: Arc<super::super::Options>,
    pub(super) leaf: VlessOutboundLeaf,
    pub(super) access: crate::transport::PreconnectPool,
    pub(super) pool: Arc<PoolOptions>,
    pub(super) profile: Arc<str>,
    pub(super) warm: NonZeroUsize,
    pub(super) originals: NonZeroUsize,
    pub(super) budget: Duration,
    pub(super) requires_lender: bool,
}
impl std::fmt::Debug for PreparedPreconnect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedVlessPreconnect")
            .field("warm", &self.warm)
            .field("originals", &self.originals)
            .finish_non_exhaustive()
    }
}
impl PreparedPreconnect {
    pub fn from_owned_options_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        input: VlessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc, TH2, THttp, TSplit>,
        runtime: &VlessTransportRuntime,
        source_dir: Option<&Path>,
        binding: UpstreamBinding,
        active: NonZeroUsize,
        budget: Duration,
        file_bytes: NonZeroUsize,
    ) -> Result<Self, RuntimeError>
    where
        TTls: zero_traits::ClientTlsProfile + ?Sized,
        TWs: zero_traits::WebSocketTransportProfile + ?Sized,
        TGrpc: zero_traits::GrpcTransportProfile + ?Sized,
        TH2: zero_traits::H2TransportProfile + ?Sized,
        THttp: zero_traits::HttpUpgradeTransportProfile + ?Sized,
        TSplit: zero_traits::SplitHttpTransportProfile + ?Sized,
    {
        if input.protocol.testpre == 0 {
            return Err(
                std::io::Error::other("preconnect requires its existing transport recipe").into(),
            );
        }
        let (leaf, _) =
            VlessOutboundLeaf::from_owned_options_refs(source_dir, input.clone(), runtime)?;
        let access = leaf
            .preconnect_access()
            .ok_or_else(|| std::io::Error::other("missing actual preconnect registry entry"))?;
        let warm = NonZeroUsize::new(access.capacity())
            .ok_or_else(|| std::io::Error::other("zero preconnect capacity"))?;
        let originals = warm
            .get()
            .checked_add(active.get())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| std::io::Error::other("preconnect original budget overflow"))?;
        let mut ordinary = input;
        ordinary.protocol.testpre = 0;
        let grpc = ordinary.grpc.is_some();
        let packet = ordinary.mkcp.is_some()
            || ordinary.protocol.quic.is_some()
            || ordinary.split_http.is_some()
            || ordinary.ws.is_some_and(|ws| {
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
        let protocol =
            super::super::Options::from_options_refs_inner(ordinary, binding.dial.clone(), true)?;
        let protocol = if grpc || packet {
            protocol
        } else {
            protocol.with_owned_material(runtime, source_dir, file_bytes)?
        };
        let profile: Arc<str> = leaf.preconnect_profile().into();
        let mut pool = access
            .control()
            .refresh()?
            .options(
                profile.clone(),
                protocol.endpoint.clone(),
                binding,
                originals,
                budget,
            )?
            .with_scoped_preparation();
        if protocol.requires_preparation_jobs() {
            pool = pool.with_preparation_jobs();
        }
        Ok(Self {
            protocol: Arc::new(protocol),
            leaf,
            access,
            pool: Arc::new(pool),
            profile,
            warm,
            originals,
            budget,
            requires_lender: grpc || packet,
        })
    }
    pub fn with_node_dns(mut self, node: ComponentId) -> Self {
        self.pool = Arc::new((*self.pool).clone().with_node_dependency(node));
        self
    }
    pub fn with_carrier_lender(mut self, dependency: ComponentId) -> Self {
        self.pool = Arc::new((*self.pool).clone().with_stream_dependency(dependency));
        self
    }
    pub fn original_limit(&self) -> NonZeroUsize {
        self.originals
    }
    pub fn pool_options(&self) -> &Arc<PoolOptions> {
        &self.pool
    }
    pub fn reuse_file_material_from(
        &mut self,
        previous: zero_core::component::ComponentInstanceId,
    ) -> Result<bool, RuntimeError> {
        match Arc::get_mut(&mut self.protocol) {
            Some(protocol) => protocol.reuse_file_material_from(previous),
            None => Ok(false),
        }
    }
    pub fn reuse_cold_file_material_from(&mut self, identity: &[u8]) -> Result<bool, RuntimeError> {
        match Arc::get_mut(&mut self.protocol) {
            Some(protocol) => protocol
                .reuse_cold_file_material_from(identity)
                .map_err(Into::into),
            None => Ok(false),
        }
    }
    pub fn remember_cold_file_material(&self, identity: &[u8]) -> Result<(), RuntimeError> {
        self.protocol
            .remember_cold_material_identity(identity)
            .map_err(Into::into)
    }
    pub fn bind(self: &Arc<Self>, pool_owner: ComponentId) -> super::super::grpc::StreamOptions {
        super::super::grpc::StreamOptions::from_preconnect(self.clone(), pool_owner)
    }
    pub(in crate::component) fn protocol_arc(&self) -> Arc<super::super::Options> {
        Arc::new(self.protocol.framing_only())
    }
    /// This actual map entry retains only weak native ownership. Canonical
    /// cold reuse may discard a proposed recipe; callers never depend on it.
    pub fn pool_handle(&self) -> crate::transport::PreconnectPool {
        self.access.clone()
    }
    pub(in crate::component) fn client_options(&self) -> Arc<super::super::mux::OwnedPoolOptions> {
        Arc::new(super::super::mux::OwnedPoolOptions::for_client(
            self.pool.clone(),
            self.protocol.clone(),
        ))
    }
}
