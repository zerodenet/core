use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::{InboundClientResponse, InboundStreamRoute, InboundStreamUdpRelay};
use zero_engine::EngineError;
use zero_traits::AsyncSocket;

use super::InboundConnectionContext;

impl InboundConnectionContext {
    /// The protocol classifies its accepted route; runtime owns TCP/UDP
    /// execution and accounting without inspecting protocol-specific variants.
    #[allow(dead_code)] // Some feature sets have no adapter using this route shape.
    pub(crate) async fn dispatch_stream_route_with_client_response<R, P>(
        self,
        route: R,
        response: P,
        udp_protocol: &'static str,
    ) -> Result<(), EngineError>
    where
        R: InboundStreamRoute,
        R::TcpStream: AsyncSocket + AsyncRead + AsyncWrite + 'static,
        P: InboundClientResponse<R::TcpStream>,
        <R::UdpRelay as InboundStreamUdpRelay>::Stream:
            AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let udp_context = self.clone();
        route
            .dispatch_inbound_route(
                move |session, stream| self.serve_with_client_response(session, stream, response),
                move |session, relay| {
                    udp_context.run_stream_udp_relay(session, relay, udp_protocol)
                },
            )
            .await
    }
}
