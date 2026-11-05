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
                ca_max_bytes,
                cap: carrier_limit,
                business: Some(Business {
                    binding: None,
                    cleanup_budget,
                }),
            },
            Arc::default(),
        )?;
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::with_capacity(16);
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            tag.hash(&mut hash);
            options.base.identity.hash(&mut hash);
            options.base.server.hash(&mut hash);
            format!("{:?}", options.grpc).hash(&mut hash);
            carrier_limit.hash(&mut hash);
            cleanup_budget.hash(&mut hash);
            streams.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        options.identity = identity.into();
        options.node_streams = streams;
        options.stream_admission = streams.then(|| runtime.grpc_carrier_state());
        options.state = runtime.grpc_datagram_state(options.identity.clone(), options.state)?;
        Ok(options)
    }
}
