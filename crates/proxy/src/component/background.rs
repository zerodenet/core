//! Retained providers hold only the actual network authority and weak services.
//! Each open borrows the current host for selection, then releases that borrow.
use super::instance::invalid;
use crate::runtime::current_services::CurrentServiceSource;
use std::sync::{Arc, OnceLock, Weak};
use zero_core::{component::ComponentInstanceId, request::Destination};
use zero_dns::DnsSystem;
use zero_platform_tokio::{
    contract::{
        dns::{DnsGeneration, DnsService},
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        stream::BoxStream,
        BoxFuture, ChannelError, Reason,
    },
    EgressInterfaceControl,
};

#[cfg(feature = "vless")]
pub(crate) mod grpc;
mod pending;
pub(crate) mod pool;
pub(crate) mod stateless;

pub(crate) struct BackgroundNetwork {
    pub(crate) egress: EgressInterfaceControl,
    pub(super) resolver: OnceLock<Weak<DnsSystem>>,
    pub(crate) native_dns: Arc<zero_dns::native::NativeDnsSource>,
    pub(crate) query_dns: Arc<zero_dns::native::NativeDnsSource>,
    pub(crate) dns_budget: std::sync::Mutex<std::sync::Weak<super::native_dns::Budget>>,
    services: OnceLock<Weak<CurrentServiceSource>>,
}
impl BackgroundNetwork {
    pub(super) fn current_services(&self) -> std::io::Result<Arc<CurrentServiceSource>> {
        self.services.get().and_then(Weak::upgrade).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "runtime service owner retired",
            )
        })
    }
    pub(crate) fn new(egress: EgressInterfaceControl) -> Self {
        Self {
            egress,
            resolver: OnceLock::new(),
            native_dns: Arc::default(),
            query_dns: Arc::default(),
            dns_budget: Default::default(),
            services: OnceLock::new(),
        }
    }
    pub(crate) fn install(&self, resolver: &Arc<DnsSystem>, source: Weak<CurrentServiceSource>) {
        assert!(self.resolver.set(Arc::downgrade(resolver)).is_ok());
        assert!(self.services.set(source).is_ok());
    }
}
pub(crate) struct BackgroundCarrier {
    pub(crate) network: Arc<BackgroundNetwork>,
    pub(crate) instance: ComponentInstanceId,
    pub(crate) initial: UpstreamBinding,
    pub(crate) endpoint: Destination,
    pub(crate) wire_endpoint: Destination,
    pub(crate) tag: String,
}
impl BackgroundCarrier {
    pub(crate) fn transport(self) -> UpstreamTransport {
        UpstreamTransport::new(self.initial.clone(), Arc::new(self))
    }
}
impl UpstreamOpener for BackgroundCarrier {
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.declared_pending_capacity(binding).ok()
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.validate_pending_original(binding, adoption)
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "select the current provider before packet acquisition",
            ))
        })
    }

    fn carrier_dns_generation(&self) -> Option<DnsGeneration> {
        self.network
            .resolver
            .get()?
            .upgrade()
            .map(|resolver| resolver.generation())
    }
    fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.select(binding, call, None)
    }
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.select(binding, call, Some(waiting))
    }
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if call.cancel.is_cancelled()
                || binding.dial != self.initial.dial
                || binding.network_generation.0 != self.network.egress.generation()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "background packet target binding changed",
                ));
            }
            let source = self.network.current_services().map_err(|source| {
                invalid(Reason::Revoked, "packet target source retired").with_source(source)
            })?;
            let services = source.runtime_services().map_err(|source| {
                invalid(Reason::Revoked, "packet target services retired").with_source(source)
            })?;
            // Preserve the actual identity/call. This delegates metadata DNS to
            // the existing host source; it never selects or opens a new socket.
            services
                .upstream_observed(&self.tag)
                .resolve_inner_packet_target(destination, identity, call)
                .await
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "select the current background provider before acquisition",
            ))
        })
    }
}

impl BackgroundCarrier {
    fn select(
        &self,
        selected: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            let invalid = || {
                invalid(
                    Reason::StalePlan,
                    "background carrier requires the actual current host and network",
                )
            };
            let source = self
                .network
                .services
                .get()
                .and_then(Weak::upgrade)
                .ok_or_else(invalid)?;
            let services = source
                .runtime_services()
                .map_err(|error| invalid().with_source(error))?;
            let binding = selected.unwrap_or_else(|| UpstreamBinding {
                prefix: vec![],
                dial: self.initial.dial.clone(),
                network_generation: zero_core::component::NetworkGeneration(
                    self.network.egress.generation(),
                ),
            });
            if call.session.is_some()
                || call.cancel.is_cancelled()
                || binding.dial != self.initial.dial
                || binding.network_generation.0 != self.network.egress.generation()
            {
                return Err(invalid());
            }
            if !binding.prefix.is_empty() && binding.dial != Default::default() {
                return Err(super::instance::invalid(
                    Reason::Unsupported,
                    "a logical prefix cannot enforce additional native dial constraints",
                ));
            }
            if binding.prefix.is_empty() {
                let opener = super::stream_bridge::network::NodeOpener::new(
                    services.upstream_observed(&self.tag),
                    self.endpoint.clone(),
                )
                .with_pending(binding.clone(), waiting)
                .with_service_call(call);
                Ok(UpstreamTransport::new(binding, Arc::new(opener)))
            } else {
                let nodes = services.protocols().background_nodes(&services, &binding)?;
                services
                    .components
                    .as_ref()
                    .map_err(|source| invalid().with_source(source.clone()))?
                    .authorize_carrier_prefix(self.instance, &binding.prefix)?;
                super::stream_bridge::relay::background::BackgroundPath {
                    nodes,
                    binding,
                    endpoint: self.endpoint.clone(),
                    wire_endpoint: self.wire_endpoint.clone(),
                    final_tag: &self.tag,
                    call,
                }
                .select_observed(&services, waiting)
                .await
            }
        })
    }
}
