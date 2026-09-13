use std::io;
use std::path::Path;

use crate::RuntimeError;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::{
    GrpcTransportProfile, H2TransportProfile, ServerTlsProfile, WebSocketTransportProfile,
};

#[cfg(feature = "grpc")]
use crate::grpc;
#[cfg(feature = "h2")]
use crate::h2;
#[cfg(feature = "ws")]
use crate::ws;

#[derive(Clone, Copy)]
pub struct InboundStreamStack<'a, TWs, TGrpc, TH2>
where
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
{
    pub ws: Option<&'a TWs>,
    pub grpc: Option<&'a TGrpc>,
    pub h2: Option<&'a TH2>,
}

pub fn build_optional_tls_acceptor<TTls>(
    source_dir: Option<&Path>,
    tls: Option<&TTls>,
) -> Result<Option<crate::tls::TlsAcceptor>, RuntimeError>
where
    TTls: ServerTlsProfile + ?Sized,
{
    tls.map(|tls| crate::tls::build_tls_acceptor(tls, source_dir))
        .transpose()
}

pub fn build_required_tls_acceptor<TTls>(
    source_dir: Option<&Path>,
    tls: Option<&TTls>,
    missing_message: &'static str,
) -> Result<crate::tls::TlsAcceptor, RuntimeError>
where
    TTls: ServerTlsProfile + ?Sized,
{
    build_optional_tls_acceptor(source_dir, tls)?.ok_or_else(|| {
        RuntimeError::Io(io::Error::new(io::ErrorKind::InvalidInput, missing_message))
    })
}

pub async fn accept_tls_inbound_stream(
    socket: TokioSocket,
    tls_acceptor: &crate::tls::TlsAcceptor,
) -> Result<crate::tls::InboundTlsStream<TokioSocket>, RuntimeError> {
    crate::tls::accept_tls_inbound(socket, tls_acceptor).await
}

pub async fn accept_tls_inbound_stream_stack<TWs, TGrpc, TH2>(
    socket: TokioSocket,
    tls_acceptor: &crate::tls::TlsAcceptor,
    stack: InboundStreamStack<'_, TWs, TGrpc, TH2>,
    invalid_message: &'static str,
) -> Result<TcpRelayStream, RuntimeError>
where
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
{
    let tls = accept_tls_inbound_stream(socket, tls_acceptor).await?;
    accept_inbound_stream_stack(tls, stack, invalid_message).await
}

pub async fn accept_inbound_stream_stack<S, TWs, TGrpc, TH2>(
    stream: S,
    stack: InboundStreamStack<'_, TWs, TGrpc, TH2>,
    invalid_message: &'static str,
) -> Result<TcpRelayStream, RuntimeError>
where
    S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
{
    let InboundStreamStack {
        ws: ws_config,
        grpc: grpc_config,
        h2: h2_config,
    } = stack;

    #[cfg(not(feature = "h2"))]
    if h2_config.is_some() {
        return invalid_inbound_stack(invalid_message);
    }

    match (ws_config, grpc_config, h2_config) {
        #[cfg(feature = "ws")]
        (Some(config), None, None) => Ok(TcpRelayStream::new(
            ws::accept_ws_profile(stream, config).await?,
        )),
        #[cfg(feature = "grpc")]
        (None, Some(config), None) => Ok(TcpRelayStream::new(
            grpc::accept_grpc_with_profile(stream, config).await?,
        )),
        #[cfg(feature = "h2")]
        (None, None, Some(config)) => Ok(TcpRelayStream::new(h2::accept_h2(stream, config).await?)),
        (None, None, None) => Ok(match stream.transport_bypass_control() {
            Some(control) => TcpRelayStream::with_transport_bypass_control(stream, control),
            None => TcpRelayStream::new(stream),
        }),
        _ => invalid_inbound_stack(invalid_message),
    }
}

fn invalid_inbound_stack(invalid_message: &'static str) -> Result<TcpRelayStream, RuntimeError> {
    Err(RuntimeError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        invalid_message,
    )))
}
