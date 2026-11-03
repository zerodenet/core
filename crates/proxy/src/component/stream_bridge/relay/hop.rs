//! An ordinary Stream role over one already selected compatibility prefix.
//! This bridge grants no root sockets, background borrowing or native receipt.
use crate::{
    component::{instance::invalid, legacy_stream::LegacyStream},
    inventory::{PreparedTcpRelayHop, StreamRoleRef},
    protocol_registry::{TcpExecutionServices, UpstreamConnectServices},
    runtime::tcp_dispatch::operation::LazyTcpRelayCarrier,
    transport::{ClientStream, TcpRelayStream},
};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use zero_core::{component::NetworkGeneration, request::Destination};
use zero_platform_tokio::contract::{
    context::OpenContext,
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::{BoxStream, ConnectRequest},
    BoxFuture, ChannelError, DeliveryState, Reason, RetryClass,
};

pub(crate) async fn connect(
    services: &TcpExecutionServices,
    hop: &PreparedTcpRelayHop,
    selected: &StreamRoleRef,
    destination: Destination,
    carrier: LazyTcpRelayCarrier<'static>,
) -> Result<TcpRelayStream, ChannelError> {
    let invocation = services.invocation.clone().ok_or_else(|| {
        invalid(
            Reason::Unsupported,
            "standard mixed relay requires actual request facts",
        )
    })?;
    let slot = selected.slot()?;
    // Admission/start happens before polling the prefix. A retired original
    // never acquires a replacement carrier through this compatibility bridge.
    let role = slot
        .stream(
            invocation
                .deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
        )
        .await
        .map_err(|mut error| {
            error.component.get_or_insert(selected.instance().component);
            error.session = error.session.or(invocation.identity.session());
            error
        })?;
    let network = services.upstream_observed(&hop.tag);
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(network.network_generation()),
    };
    let endpoint = Destination::new(
        match zero_core::address::parse_ip_literal(&hop.server) {
            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            None => zero_core::Address::Domain(hop.server.clone()),
        },
        hop.port,
    );
    let opener = Arc::new(SelectedPrefix {
        carrier: tokio::sync::Mutex::new(Some(carrier)),
        endpoints: Mutex::new(None),
        binding: binding.clone(),
        endpoint,
        session: invocation.identity.session(),
        network,
    });
    let context = OpenContext {
        identity: invocation.identity.clone(),
        deadline: invocation.deadline,
        cancel: invocation.cancellation(),
        observer: None,
        upstream: UpstreamTransport::new(binding, opener.clone()),
    };
    let connected = role
        .connect(ConnectRequest { destination }, context)
        .await
        .map_err(|mut error| {
            // A compatibility prefix has no invented native RoleIds. Even if the
            // final handshake sent no CONNECT, an earlier hop may have submitted.
            error.delivery = DeliveryState::Unknown;
            error.retry = RetryClass::Never;
            error
        })?;
    let (local, peer) = opener
        .endpoints
        .lock()
        .expect("relay endpoints poisoned")
        .take()
        .ok_or_else(|| {
            invalid(
                Reason::Internal,
                "standard relay did not consume its selected prefix",
            )
        })?;
    Ok(TcpRelayStream::from_client(LegacyStream::with_endpoints(
        connected.io,
        local,
        peer,
    )))
}

type Endpoints = (Option<SocketAddr>, Option<SocketAddr>);
struct SelectedPrefix {
    carrier: tokio::sync::Mutex<Option<LazyTcpRelayCarrier<'static>>>,
    endpoints: Mutex<Option<Endpoints>>,
    binding: UpstreamBinding,
    endpoint: Destination,
    session: Option<zero_core::component::SessionId>,
    network: UpstreamConnectServices,
}
impl UpstreamOpener for SelectedPrefix {
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
                "mixed Stream prefix grants no Datagram role",
            ))
        })
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != self.binding
                || destination != self.endpoint
                || call.session != self.session
                || self.network.network_generation() != self.binding.network_generation.0
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "selected mixed relay binding changed",
                ));
            }
            if call.cancel.is_cancelled() {
                return Err(invalid(
                    Reason::Cancelled,
                    "selected mixed relay was cancelled",
                ));
            }
            let carrier = self.carrier.lock().await.take().ok_or_else(|| {
                invalid(
                    Reason::Capacity,
                    "selected mixed relay carrier was already consumed",
                )
            })?;
            let stream = carrier.open().await.map_err(prefix_failure)?;
            if self.network.network_generation() != self.binding.network_generation.0 {
                return Err(invalid(
                    Reason::StalePlan,
                    "network changed during mixed relay establishment",
                ));
            }
            *self.endpoints.lock().expect("relay endpoints poisoned") =
                Some((stream.local_addr().ok(), stream.peer_addr().ok()));
            Ok(Box::pin(self.network.observe_stream(stream)) as BoxStream)
        })
    }
}

// This is the erased compatibility-to-channel error boundary, never protocol
// resume dispatch. Retain both the original typed facts and the actual source.
fn prefix_failure(source: zero_engine::EngineError) -> ChannelError {
    let mut error = if let Some(original) = crate::transport::channel_failure(&source) {
        let mut error = ChannelError::new(original.stage, original.reason, original.impact);
        error.component = original.component;
        error.session = original.session;
        error
    } else {
        ChannelError::new(
            zero_platform_tokio::contract::Stage::Dial,
            Reason::Io,
            zero_platform_tokio::contract::Impact::Session,
        )
    };
    error.delivery = DeliveryState::Unknown;
    error.retry = RetryClass::Never;
    error.with_source(source)
}
