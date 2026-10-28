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
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::BoxStream,
        BoxFuture, ChannelError, Reason,
    },
    controlled_carrier::ResolvingCarrierOpener,
    EgressInterfaceControl,
};

pub(crate) mod pool;

pub(crate) struct BackgroundNetwork {
    pub(crate) egress: EgressInterfaceControl,
    pub(super) resolver: OnceLock<Weak<DnsSystem>>,
    pub(crate) native_dns: Arc<zero_dns::native::NativeDnsSource>,
    services: OnceLock<Weak<CurrentServiceSource>>,
}
impl BackgroundNetwork {
    pub(crate) fn new(egress: EgressInterfaceControl) -> Self {
        Self {
            egress,
            resolver: OnceLock::new(),
            native_dns: Arc::default(),
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
        selected: Option<UpstreamBinding>,
        call: CarrierContext,
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
            let binding = selected.ok_or_else(invalid)?;
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
                let resolver = self
                    .network
                    .resolver
                    .get()
                    .and_then(Weak::upgrade)
                    .ok_or_else(invalid)?;
                let observer = services.outbound_io(&self.tag, zero_api::TrafficPlane::Outer);
                let opener = ResolvingCarrierOpener::new(
                    self.instance,
                    binding.clone(),
                    self.endpoint.clone(),
                    resolver,
                    Arc::new(self.network.egress.clone()),
                    observer,
                )?;
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
                .select(&services)
                .await
            }
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
