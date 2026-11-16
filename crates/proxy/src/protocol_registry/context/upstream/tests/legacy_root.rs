//! Explicit root capability for host services. Ordinary roles only receive
//! their selected UpstreamTransport, never this helper as an alternate route.
use super::UpstreamConnectServices;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        dns::{DnsQueryContext, DnsQueryRole, DnsRequest, DnsService},
        network::{CarrierContext, UpstreamBinding},
        ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    TokioSocket,
};

impl UpstreamConnectServices {
    pub(crate) async fn open_root_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        role: DnsQueryRole,
        context: CarrierContext,
    ) -> Result<TokioSocket, ChannelError> {
        let session = context.session;
        let failure = |reason| {
            let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
            error.session = session;
            error.delivery = DeliveryState::NotSubmitted;
            error
        };
        if !binding.prefix.is_empty() || binding.network_generation.0 != self.network_generation() {
            return Err(failure(Reason::StalePlan));
        }
        if !matches!(
            role,
            DnsQueryRole::Node | DnsQueryRole::Direct | DnsQueryRole::Bootstrap
        ) {
            return Err(failure(Reason::InvalidConfig));
        }
        if context.cancel.is_cancelled() {
            return Err(failure(Reason::Cancelled));
        }
        if context
            .deadline
            .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            return Err(failure(Reason::Timeout));
        }
        let family = binding
            .dial
            .effective_family()
            .map_err(|source| failure(Reason::InvalidConfig).with_source(source))?;
        let addresses = match destination.address {
            Address::Domain(name) => DnsService::resolve(
                self.resolver.as_ref(),
                DnsRequest { name, role, family },
                DnsQueryContext {
                    deadline: context.deadline,
                    cancel: context.cancel.clone(),
                    expected_generation: None,
                },
            )
            .await?
            .addresses
            .into_iter()
            .map(super::ip_address_to_std)
            .collect::<Vec<_>>(),
            Address::Ipv4(bytes) => vec![std::net::Ipv4Addr::from(bytes).into()],
            Address::Ipv6(bytes) => vec![std::net::Ipv6Addr::from(bytes).into()],
        };
        let candidates = addresses
            .into_iter()
            .map(|ip| std::net::SocketAddr::new(ip, destination.port))
            .filter_map(|peer| binding.dial.normalize_peer(peer).ok())
            .collect::<Vec<_>>();
        if binding.network_generation.0 != self.network_generation() {
            return Err(failure(Reason::StalePlan));
        }
        let socket = crate::transport::open_service_candidates(
            candidates,
            &self.egress_interface,
            &binding.dial,
            context,
        )
        .await?;
        if binding.network_generation.0 != self.network_generation() {
            return Err(failure(Reason::StalePlan));
        }
        Ok(socket.with_observer(self.observer()))
    }
}
