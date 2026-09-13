use std::io;
use std::path::Path;

use crate::RuntimeError;
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile,
    WebSocketTransportProfile,
};

#[cfg(feature = "grpc")]
use crate::grpc;
#[cfg(feature = "h2")]
use crate::h2;
#[cfg(feature = "http_upgrade")]
use crate::http_upgrade;
#[cfg(feature = "tls")]
use crate::tls;
#[cfg(feature = "ws")]
use crate::ws;

#[derive(Clone, Copy)]
pub struct StreamTransportStack<'a, TTls, TWs, TGrpc, TH2, THttp>
where
    TTls: ClientTlsProfile + ?Sized,
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
    THttp: HttpUpgradeTransportProfile + ?Sized,
{
    pub tls: Option<&'a TTls>,
    pub ws: Option<&'a TWs>,
    pub grpc: Option<&'a TGrpc>,
    pub h2: Option<&'a TH2>,
    pub http_upgrade: Option<&'a THttp>,
    pub source_dir: Option<&'a Path>,
}

pub async fn connect_socket_transport_stack<TTls, TWs, TGrpc, TH2, THttp>(
    socket: TokioSocket,
    stack: StreamTransportStack<'_, TTls, TWs, TGrpc, TH2, THttp>,
    server: &str,
    port: u16,
    invalid_message: &'static str,
) -> Result<TcpRelayStream, RuntimeError>
where
    TTls: ClientTlsProfile + ?Sized,
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
    THttp: HttpUpgradeTransportProfile + ?Sized,
{
    #[cfg(feature = "tls")]
    let carrier = match stack.tls {
        Some(tls) => tls::connect_tls_upstream(socket, tls, stack.source_dir, server).await?,
        None => TcpRelayStream::new(socket),
    };

    #[cfg(not(feature = "tls"))]
    let carrier = {
        if stack.tls.is_some() {
            return invalid_transport_stack(invalid_message);
        }
        let _ = stack.source_dir;
        TcpRelayStream::new(socket)
    };

    connect_layered_transport_stack(carrier, stack, server, port, invalid_message).await
}

pub async fn connect_relay_transport_stack<TTls, TWs, TGrpc, TH2, THttp>(
    stream: TcpRelayStream,
    stack: StreamTransportStack<'_, TTls, TWs, TGrpc, TH2, THttp>,
    server: &str,
    port: u16,
    invalid_message: &'static str,
) -> Result<TcpRelayStream, RuntimeError>
where
    TTls: ClientTlsProfile + ?Sized,
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
    THttp: HttpUpgradeTransportProfile + ?Sized,
{
    #[cfg(feature = "tls")]
    let carrier = match stack.tls {
        Some(tls) => tls::connect_tls_stream(stream, tls, stack.source_dir, server).await?,
        None => stream,
    };

    #[cfg(not(feature = "tls"))]
    let carrier = {
        if stack.tls.is_some() {
            return invalid_transport_stack(invalid_message);
        }
        let _ = stack.source_dir;
        stream
    };

    connect_layered_transport_stack(carrier, stack, server, port, invalid_message).await
}

async fn connect_layered_transport_stack<TTls, TWs, TGrpc, TH2, THttp>(
    carrier: TcpRelayStream,
    stack: StreamTransportStack<'_, TTls, TWs, TGrpc, TH2, THttp>,
    _server: &str,
    _port: u16,
    invalid_message: &'static str,
) -> Result<TcpRelayStream, RuntimeError>
where
    TTls: ClientTlsProfile + ?Sized,
    TWs: WebSocketTransportProfile + ?Sized,
    TGrpc: GrpcTransportProfile + ?Sized,
    TH2: H2TransportProfile + ?Sized,
    THttp: HttpUpgradeTransportProfile + ?Sized,
{
    #[cfg(feature = "grpc")]
    let grpc_authority = stack
        .tls
        .and_then(ClientTlsProfile::server_name)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if let Ok(ip) = _server.parse::<std::net::IpAddr>() {
                std::net::SocketAddr::new(ip, _port).to_string()
            } else {
                _server.to_owned()
            }
        });
    let StreamTransportStack {
        ws: ws_config,
        grpc: grpc_config,
        h2: h2_config,
        http_upgrade: http_upgrade_config,
        ..
    } = stack;

    #[cfg(not(feature = "h2"))]
    if h2_config.is_some() {
        return invalid_transport_stack(invalid_message);
    }

    #[cfg(not(feature = "http_upgrade"))]
    if http_upgrade_config.is_some() {
        return invalid_transport_stack(invalid_message);
    }

    #[cfg(not(feature = "ws"))]
    if ws_config.is_some() {
        return invalid_transport_stack(invalid_message);
    }

    #[cfg(not(feature = "grpc"))]
    if grpc_config.is_some() {
        return invalid_transport_stack(invalid_message);
    }

    #[cfg(feature = "http_upgrade")]
    if let Some(config) = http_upgrade_config {
        if ws_config.is_some() || grpc_config.is_some() || h2_config.is_some() {
            return invalid_transport_stack(invalid_message);
        }
        return Ok(TcpRelayStream::new(
            http_upgrade::connect_http_upgrade(carrier, config).await?,
        ));
    }

    let application_settings = carrier
        .application_settings()
        .filter(|s| s.protocol == b"h2")
        .map(|s| s.peer.clone());
    match (ws_config, grpc_config, h2_config) {
        #[cfg(feature = "ws")]
        (Some(ws), None, None) => Ok(TcpRelayStream::new(
            ws::connect_ws(carrier, ws, _server, _port).await?,
        )),
        #[cfg(feature = "grpc")]
        (None, Some(grpc), None) => Ok(TcpRelayStream::new(
            grpc::connect_grpc_with_settings(
                carrier,
                grpc,
                &grpc_authority,
                application_settings.as_deref(),
            )
            .await?,
        )),
        #[cfg(feature = "h2")]
        (None, None, Some(h2_config)) => Ok(TcpRelayStream::new(
            h2::connect_h2_with_settings(
                carrier,
                h2_config,
                _server,
                _port,
                application_settings.as_deref(),
            )
            .await?,
        )),
        (None, None, None) => Ok(carrier),
        _ => invalid_transport_stack(invalid_message),
    }
}

fn invalid_transport_stack(invalid_message: &'static str) -> Result<TcpRelayStream, RuntimeError> {
    Err(RuntimeError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        invalid_message,
    )))
}
