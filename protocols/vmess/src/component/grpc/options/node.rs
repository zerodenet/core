//! One production Node owner; Stream admission is an explicit whole-config choice.
use super::*;
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    sync::OnceLock,
};
impl Options {
    pub fn from_node_options_refs<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        carrier_limit: NonZeroUsize,
        cleanup_budget: Duration,
        runtime: &crate::transport::VmessTransportRuntime,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self::from_node_options_refs_at(
            input,
            dial,
            None,
            None,
            carrier_limit,
            cleanup_budget,
            runtime,
        )
    }
    /// Cold Node Datagram recipe with an explicit bounded CA file grant.
    /// File material is read by the installed lifecycle owner during start.
    pub fn from_node_options_refs_at<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
        carrier_limit: NonZeroUsize,
        cleanup_budget: Duration,
        runtime: &crate::transport::VmessTransportRuntime,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self::from_node_options_refs_with_streams_at(
            input,
            dial,
            source_dir,
            ca_max_bytes,
            carrier_limit,
            cleanup_budget,
            runtime,
            false,
        )
    }
    /// The host may enable Streams only after excluding every compatibility
    /// Stream caller for this complete owning configuration.
    #[expect(
        clippy::too_many_arguments,
        reason = "same Node recipe plus explicit whole-config admission"
    )]
    pub fn from_node_options_refs_with_streams_at<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
        carrier_limit: NonZeroUsize,
        cleanup_budget: Duration,
        runtime: &crate::transport::VmessTransportRuntime,
        streams: bool,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self::node_options_inner(
            input,
            dial,
            source_dir,
            ca_max_bytes,
            carrier_limit,
            cleanup_budget,
            runtime,
            streams,
            false,
        )
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "same owning Node recipe with its explicit resource-backed material"
    )]
    pub fn from_owned_node_options_refs_with_streams_at<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
        carrier_limit: NonZeroUsize,
        cleanup_budget: Duration,
        runtime: &crate::transport::VmessTransportRuntime,
        streams: bool,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self::node_options_inner(
            input,
            dial,
            source_dir,
            ca_max_bytes,
            carrier_limit,
            cleanup_budget,
            runtime,
            streams,
            true,
        )
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "private complete Node recipe projection"
    )]
    fn node_options_inner<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
        carrier_limit: NonZeroUsize,
        cleanup_budget: Duration,
        runtime: &crate::transport::VmessTransportRuntime,
        streams: bool,
        owned: bool,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let tag = input.tag.to_owned();
        if cleanup_budget.is_zero() {
            return Err(Error::Config(
                "controlled VMess carrier retirement budget must be nonzero",
            ));
        }
        // Independent per-recipe admission, with no legacy clients to adopt.
        // Only this owner is constructed; no second protocol runtime or pool.
        let mut options = Self::prepare(
            input,
            dial,
            source_dir,
            Limits {
                owned,
                ca_max_bytes,
                cap: carrier_limit,
                business: Some(Business {
                    binding: None,
                    cleanup_budget,
                }),
            },
            Arc::default(),
        )?;
        if owned {
            options.base = Arc::new(
                (*options.base)
                    .clone()
                    .with_owned_material(
                        runtime,
                        source_dir,
                        ca_max_bytes.unwrap_or(NonZeroUsize::MIN),
                    )
                    .map_err(|_| Error::Config("invalid owned VMess TLS recipe"))?,
            );
        }
        options.identity_tag = Some(tag.into());
        options.node_streams = streams;
        options.stream_admission = streams.then(|| runtime.grpc_carrier_state());
        options.refresh_node_identity();
        options.state = runtime.grpc_datagram_state(options.identity.clone(), options.state)?;
        Ok(options)
    }
    fn refresh_node_identity(&mut self) {
        let Some(tag) = &self.identity_tag else {
            return;
        };
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::with_capacity(16);
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            tag.hash(&mut hash);
            self.base.identity.hash(&mut hash);
            self.base.server.hash(&mut hash);
            format!("{:?}", self.grpc).hash(&mut hash);
            self.cap.hash(&mut hash);
            self.business
                .as_ref()
                .map(|b| b.cleanup_budget)
                .hash(&mut hash);
            self.node_streams.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        if let Some(dependency) = self.dns_dependency {
            identity.extend_from_slice(&dependency.0.to_le_bytes());
        }
        self.identity = identity.into();
    }
    pub fn reuse_file_material_from(
        &mut self,
        previous: zero_core::component::ComponentInstanceId,
    ) -> Result<bool, zero_transport::RuntimeError> {
        let mut base = (*self.base).clone();
        let inherited = base.reuse_file_material_from(previous)?;
        if inherited {
            self.base = Arc::new(base);
            self.refresh_node_identity();
        }
        Ok(inherited)
    }
}
