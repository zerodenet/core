//! Fixed server diagnostics use the same Direct role and Node candidate path.
//! This contains no Session, second connector, native owner, Scope or pool.
use super::*;
use crate::{
    component::instance::{invalid, retained},
    protocol_registry::{TcpExecutionServices, UpstreamConnectServices},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    network::{CarrierContext, ControlledStreamRequest, UpstreamOpener},
    stream::BoxStream,
    BoxFuture,
};
struct Target {
    services: UpstreamConnectServices,
    destination: Destination,
    host: String,
    probe: controlled::ProbeRequest,
}
impl UpstreamOpener for Target {
    fn supports_controlled_stream(&self) -> bool {
        true
    }
    fn open_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if !binding.prefix.is_empty()
                || binding.network_generation.0 != self.services.network_generation()
                || request.destination != self.destination
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "fixed target diagnostic changed actual endpoint/generation",
                ));
            }
            let demand = controlled::DirectDemand::Probe(self.probe.clone());
            demand.check_direct_destination(&request.destination)?;
            demand.check_direct_owner(
                &binding.dial,
                &request.grants.registration(),
                &request.tasks,
            )?;
            let candidates = controlled::ControlledCandidates::new(
                controlled::ControlledDemand {
                    request: demand,
                    grants: request.grants,
                    tasks: request.tasks,
                    retirement: request.retirement,
                },
                request.context,
                self.services.clone(),
                binding,
            )?;
            // The original fixed-server probe used Node DNS, default dial policy and
            // the existing candidate scheduler. Keep all three, including the same
            // egress/source-interface selection and bounded parallel candidates.
            self.services
                .connect_inbound_target_using(&self.host, self.destination.port, &candidates)
                .await
                .map(|io| Box::pin(io) as BoxStream)
                .map_err(|source| {
                    ChannelError::new(
                        zero_platform_tokio::contract::Stage::Dial,
                        Reason::Io,
                        zero_platform_tokio::contract::Impact::Session,
                    )
                    .with_source(source)
                })
        })
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
        // This private connect-only helper advertises only controlled Stream.
        // No Datagram role or native grant is created/published by this path.
        Box::pin(async {
            Err(invalid(
                Reason::Internal,
                "fixed TCP target diagnostic has no declared datagram role",
            ))
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
                "fixed target diagnostic requires its original controlled Direct role",
            ))
        })
    }
}
pub(crate) async fn connect_target(
    services: &TcpExecutionServices,
    host: String,
    port: u16,
    call: zero_runtime::ProbeCall,
) -> Result<(), ChannelError> {
    call.check_open()?;
    let plan = services
        .components
        .as_ref()
        .map_err(|source| retained(source.clone()))?;
    let slot = plan.direct(None)?;
    let role = slot.stream(call.deadline()).await?;
    let destination = Destination::new(
        match host.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Ok(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            Err(_) => zero_core::Address::Domain(host.clone()),
        },
        port,
    );
    let invocation: Invocation = ServiceInvocation {
        caller: Some(call.clone()),
        deadline: call.deadline(),
        purpose: ServicePurpose::DiagnosticProbe,
    }
    .into();
    let probe = controlled::ProbeRequest::new(&slot, &invocation, destination.clone())?
        .ok_or_else(|| {
            invalid(
                Reason::InvalidConfig,
                "fixed target diagnostic lacks its original ProbeCall",
            )
        })?;
    let upstream = services.upstream();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: zero_traits::DialPolicy::default(),
        network_generation: zero_core::component::NetworkGeneration(upstream.network_generation()),
    };
    let opener = Arc::new(Target {
        services: upstream,
        destination: destination.clone(),
        host,
        probe,
    });
    let connected = role
        .connect(
            zero_platform_tokio::contract::stream::ConnectRequest { destination },
            zero_platform_tokio::contract::context::OpenContext {
                identity: invocation.identity,
                deadline: Some(call.deadline()),
                cancel: call.cancellation(),
                observer: None,
                upstream: UpstreamTransport::new(binding, opener),
            },
        )
        .await?;
    // TCP connect-only: no HTTP/TLS/SOCKS bytes. The actual ProbeCall retains
    // its original pending controller until Native/Users/join/ACK cleanup.
    drop(connected);
    Ok(())
}
