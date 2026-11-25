//! Protocol-owned ordinary Trojan Stream/Datagram roles hosted via
//! `zero_transport::runner::StatelessOutboundFactory`.

mod datagram;
pub mod inbound;
pub mod mux;
mod options;
pub use options::Options;

use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    context::ConnectFact,
    lifecycle::BuildError,
    stream::{BoxStream, ConnectRequest, Connected, PeerConfirmation},
    ChannelError, DeliveryState, Stage,
};
use zero_traits::DialPolicy;
use zero_transport::{
    runner::{
        GuardedCall, HandshakeError, StatelessOutboundFactory, StatelessOutboundOptions,
        StatelessTransportStack, StreamProtocolHandshake,
    },
    TcpRelayStream,
};

impl StatelessOutboundOptions for Options {
    fn endpoint(&self) -> &Destination {
        &self.endpoint
    }

    fn dial(&self) -> &DialPolicy {
        &self.dial
    }

    fn static_identity(&self) -> Arc<[u8]> {
        self.identity.clone()
    }

    fn supports_datagram(&self) -> bool {
        true
    }

    fn browser(&self) -> Option<&Arc<zero_transport::browser_dialer::component::Options>> {
        self.browser.as_ref()
    }

    fn transport_stack(&self) -> Option<StatelessTransportStack<'_>> {
        let mut stack = StatelessTransportStack::new(
            &self.server,
            self.endpoint.port,
            "trojan: ws and grpc are mutually exclusive",
        );
        stack.tls = Some(&self.tls);
        stack.ws = self.ws.as_ref();
        stack.grpc = self.grpc.as_ref();
        Some(stack)
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct TrojanProtocol;

impl StreamProtocolHandshake for TrojanProtocol {
    type Options = Options;
    type Prepared = Vec<u8>;

    fn prepare(
        &self,
        options: &Options,
        request: &ConnectRequest,
    ) -> Result<Vec<u8>, HandshakeError> {
        crate::shared::build_request_with_hash(
            &options.password,
            &request.destination.address,
            request.destination.port,
            crate::shared::CMD_TCP,
        )
        .map_err(HandshakeError::invalid_config)
    }

    async fn handshake(
        &self,
        _options: &Options,
        mut io: TcpRelayStream,
        bytes: Vec<u8>,
        call: &GuardedCall<'_>,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        call.run_io(Stage::Handshake, DeliveryState::Unknown, async {
            io.write_all(&bytes).await?;
            io.flush().await
        })
        .await?;
        let facts = call.facts();
        // WebSocket early data can defer the upgrade until write/flush.
        // Only successful flush proves that the complete carrier is ready.
        facts.record(ConnectFact::CarrierReady);
        facts.record(ConnectFact::RequestSubmitted);
        facts.protocol_traffic(0, bytes.len() as u64);
        // Trojan sends no successful CONNECT acknowledgement. write/flush is
        // local submission only; the first business bytes may expose rejection.
        Ok(Connected {
            io: Box::pin(io),
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}

pub struct Factory;

impl ComponentFactory for Factory {
    type Options = Options;

    fn describe(&self, options: &Options) -> ComponentSpec {
        StatelessOutboundFactory::with_datagram(TrojanProtocol, TrojanProtocol).describe(options)
    }

    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        StatelessOutboundFactory::with_datagram(TrojanProtocol, TrojanProtocol)
            .build(options, context)
    }
}
