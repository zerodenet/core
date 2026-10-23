//! Classify one physical TCP carrier before handing business identity to host.
use super::*;
use crate::inbound::AuthenticatedClient;
use plan::VlessTcpInboundAcceptResult;
use zero_core::{request::Destination, InboundRouteAccept, SessionAuth};

type Recorded = zero_transport::MeteredStream<zero_transport::RecordingStream<TcpRelayStream>>;
type Legacy =
    InboundRouteAccept<crate::inbound::VlessAcceptedClientRoute<Recorded>, VlessTcpFallbackReplay>;

pub enum VlessStandardTcpAccept {
    Tcp {
        destination: Destination,
        auth: SessionAuth,
        sni: Option<String>,
        stream: Box<Recorded>,
    },
    Legacy(Box<Legacy>),
}
impl VlessInboundListenerRequest {
    /// This accepts ordinary non-multiplexed TCP transports. Vision, Zero AEAD,
    /// UDP/MUX and reverse control preserve their existing codec and executor.
    pub async fn accept_standard_tcp_route(
        self,
        socket: TokioSocket,
    ) -> Result<Option<VlessStandardTcpAccept>, RuntimeError> {
        let Self {
            profile,
            transport,
            fallback_enabled,
            mux_response_backlog,
            ..
        } = self;
        let decryption = transport.decryption.clone();
        let policy = transport.fallback_policy.clone();
        let source = socket.peer_addr().ok();
        let destination = socket.local_addr().ok();
        let Some(accepted) = transport.accept_tcp_inbound(socket).await? else {
            return Ok(None);
        };
        let route = match accepted {
            VlessTcpInboundAcceptResult::Control(control) => InboundRouteAccept::Control(control),
            VlessTcpInboundAcceptResult::FallbackReplay(replay) => {
                if !fallback_enabled {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "fallback replay requires fallback config",
                    )
                    .into());
                }
                InboundRouteAccept::Fallback(VlessTcpFallbackReplay::Client(replay))
            }
            VlessTcpInboundAcceptResult::Stream { stream, sni, alpn } => {
                let stream = session::decrypt_stream(decryption.as_ref(), stream).await?;
                match profile
                    .accept_standard_client_owned(record_client_stream(stream))
                    .await
                {
                    Ok(AuthenticatedClient::Tcp(tcp)) => {
                        return Ok(Some(VlessStandardTcpAccept::Tcp {
                            destination: tcp.destination,
                            auth: tcp.auth,
                            sni,
                            stream: Box::new(tcp.stream),
                        }));
                    }
                    Ok(AuthenticatedClient::Legacy(accepted)) => match profile.prepare_reverse(
                        (*accepted).with_inbound_endpoints(source, destination),
                        mux_response_backlog,
                    ) {
                        Ok(control) => InboundRouteAccept::Control(control),
                        Err(accepted) => InboundRouteAccept::Route(
                            accepted
                                .into_route_with_sni(sni, mux_response_backlog)
                                .await?,
                        ),
                    },
                    Err(rejected) => {
                        let (auth_error, mut replay) = rejected.into_fallback_replay();
                        if !fallback_enabled {
                            return Err(auth_error.into());
                        }
                        replay
                            .select_route(
                                &policy,
                                sni.as_deref(),
                                alpn.as_deref(),
                                source,
                                destination,
                            )
                            .await?;
                        InboundRouteAccept::Fallback(VlessTcpFallbackReplay::Client(replay))
                    }
                }
            }
        };
        Ok(Some(VlessStandardTcpAccept::Legacy(Box::new(route))))
    }
}
