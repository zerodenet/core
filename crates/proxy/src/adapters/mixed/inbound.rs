use ::socks5::transport::{Socks5InboundAcceptor, Socks5InboundUserRef};
use zero_engine::EngineError;
use zero_traits::AsyncSocket;

use crate::adapters::http::inbound::{handle_http_connection, HttpConnectInboundHandler};
use crate::adapters::socks5::inbound::handle_socks5_connection;
use crate::runtime::inbound_operation::{InboundConnectionContext, TcpInboundListenerOperation};
use crate::transport::{MeteredStream, PrefixedSocket, TcpRelayStream};

#[derive(Clone)]
pub(crate) struct MixedInboundRequest {
    pub(crate) socks5_acceptor: Socks5InboundAcceptor,
    pub(crate) http_handler: HttpConnectInboundHandler,
}

async fn handle_mixed_connection(
    mut stream: zero_platform_tokio::TokioSocket,
    request: MixedInboundRequest,
    context: InboundConnectionContext,
) -> Result<(), EngineError> {
    // Detect protocol from first byte.
    let mut first = [0_u8; 1];
    if stream.read(&mut first).await? == 0 {
        return Ok(());
    }
    let first_byte = first[0];
    let relay_stream = prefixed_relay_stream(stream, first_byte);

    if socks5::is_socks5_greeting_byte(first_byte) {
        handle_socks5_connection(
            context,
            MeteredStream::new(relay_stream),
            &request.socks5_acceptor,
        )
        .await
    } else {
        handle_http_connection(relay_stream, request.http_handler, context).await
    }
}

fn prefixed_relay_stream(
    stream: zero_platform_tokio::TokioSocket,
    first_byte: u8,
) -> TcpRelayStream {
    let local_addr = stream.local_addr().ok();
    let prefixed = PrefixedSocket::from_byte(stream, first_byte);

    match local_addr {
        Some(addr) => TcpRelayStream::with_local_addr(prefixed, addr),
        None => TcpRelayStream::new(prefixed),
    }
}

impl crate::adapters::mixed::MixedAdapter {
    pub(super) fn prepare_inbound_listener_impl(
        &self,
        inbound: zero_config::InboundConfig,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let zero_config::InboundProtocolConfig::Mixed { socks5_users } = &inbound.protocol else {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "mixed adapter received non-mixed inbound config",
            )));
        };
        let request = MixedInboundRequest {
            socks5_acceptor: Socks5InboundAcceptor::from_options_refs(socks5_users.iter().map(
                |user| Socks5InboundUserRef {
                    username: user.username.as_str(),
                    password: user.password.as_str(),
                    principal_key: user.principal_key.as_deref(),
                    up_bps: user.up_bps,
                    down_bps: user.down_bps,
                },
            )),
            http_handler: HttpConnectInboundHandler::default(),
        };
        Ok(Box::new(TcpInboundListenerOperation {
            protocol_name: "mixed",
            error_protocol_name: "mixed",
            request,
            dispatch: |request: MixedInboundRequest, socket, context: InboundConnectionContext| async move {
                handle_mixed_connection(socket, request, context).await
            },
        }))
    }
}
