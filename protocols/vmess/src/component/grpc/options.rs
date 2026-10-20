use super::{
    registry::{Recipe, State},
    *,
};
use std::{num::NonZeroUsize, path::Path, time::Duration};
use zero_core::Error;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_traits::{ClientTlsProfile, DialPolicy, GrpcTransportProfile, WebSocketTransportProfile};
use zero_transport::profile::OwnedGrpcProfile;
/// Explicit library recipe for the adapter's one real admission state.
/// There is no client cache, production default budget, or legacy adoption.
pub struct Options {
    pub(super) base: Arc<super::super::Options>,
    pub(super) state: Arc<State>,
    pub(super) grpc: OwnedGrpcProfile,
    pub(super) authority: String,
    pub(super) cap: NonZeroUsize,
    pub(super) identity: Arc<[u8]>,
    pub(super) business: Option<Business>,
}
impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VmessGrpcOptions")
            .field("carrier_limit", &self.cap)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct Business {
    pub(super) binding: UpstreamBinding,
    /// Explicit independent retirement budget; never a demand handshake timer.
    pub(super) cleanup_budget: Duration,
}
struct Limits {
    ca_max_bytes: Option<NonZeroUsize>,
    cap: NonZeroUsize,
    business: Option<Business>,
}
impl Options {
    pub fn from_options_refs_at<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
        carrier_limit: NonZeroUsize,
        runtime: &crate::transport::VmessTransportRuntime,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self::prepare(
            input,
            dial,
            source_dir,
            Limits {
                ca_max_bytes,
                cap: carrier_limit,
                business: None,
            },
            runtime,
        )
    }
    /// Explicit library Business recipe; no background carrier is granted or
    /// retained. Every connect must bring its actual host-authorized opener.
    pub fn from_business_options_refs_at<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        binding: UpstreamBinding,
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
        if !binding.prefix.is_empty() {
            return Err(Error::Unsupported(
                "controlled VMess Business gRPC requires an empty prefix",
            ));
        }
        if cleanup_budget.is_zero() {
            return Err(Error::Config(
                "controlled VMess carrier retirement budget must be nonzero",
            ));
        }
        Self::prepare(
            input,
            binding.dial.clone(),
            source_dir,
            Limits {
                ca_max_bytes,
                cap: carrier_limit,
                business: Some(Business {
                    binding,
                    cleanup_budget,
                }),
            },
            runtime,
        )
    }
    fn prepare<TTls, TWs, TGrpc>(
        input: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        limits: Limits,
        runtime: &crate::transport::VmessTransportRuntime,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        let Limits {
            ca_max_bytes,
            cap: carrier_limit,
            business,
        } = limits;
        if input.ws.is_some() {
            return Err(Error::Config("vmess: ws and grpc are mutually exclusive"));
        }
        let grpc = input
            .grpc
            .ok_or(Error::Config("explicit VMess gRPC profile required"))?;
        let grpc = OwnedGrpcProfile::from_profile(grpc);
        let tag = input.tag;
        let ordinary: crate::transport::VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc> =
            crate::transport::VmessOutboundBuildOptionsRef {
                grpc: None,
                ..input
            };
        let mut base =
            super::super::Options::from_options_refs_at(ordinary, dial, source_dir, ca_max_bytes)?;
        let ip = zero_core::address::parse_ip_literal(&base.server).ok_or(Error::Unsupported(
            "controlled VMess gRPC currently requires a literal endpoint",
        ))?;
        // No unowned TLS side channels are admitted into this carrier owner.
        if let Some(tls) = &base.tls {
            zero_transport::tls::PreparedClient::validate_profile(tls).map_err(|_| {
                Error::Unsupported("controlled VMess gRPC requires supported prepared TLS")
            })?;
        }
        let authority = base
            .tls
            .as_ref()
            .and_then(|tls| tls.server_name.as_deref())
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| std::net::SocketAddr::new(ip, base.endpoint.port).to_string());
        zero_transport::grpc::validate_single_client_profile(&grpc, &authority)
            .map_err(|_| Error::Config("invalid VMess gRPC profile"))?;
        base.grpc = Some(grpc.clone());
        let state = runtime.grpc_carrier_state();
        let identity = state.recipe(Recipe {
            tag: tag.into(),
            endpoint: base.endpoint.clone(),
            server: base.server.clone(),
            uuid: base.uuid,
            cipher: base.cipher,
            tls: base.tls.clone(),
            grpc: grpc.clone(),
            source_dir: source_dir.map(Path::to_path_buf),
            ca: base
                .ca_file
                .as_ref()
                .map(|file| (file.path.clone(), file.max_bytes)),
            dial: base.dial.clone(),
            cap: carrier_limit,
            business: business.clone(),
        })?;
        Ok(Self {
            base: Arc::new(base),
            state,
            grpc,
            authority,
            cap: carrier_limit,
            identity,
            business,
        })
    }
}
