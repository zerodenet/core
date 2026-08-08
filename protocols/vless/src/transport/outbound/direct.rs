use std::io;

use zero_platform_tokio::{TcpRelayStream, TokioSocket};

use crate::reality::{upgrade_reality_client, RealityClientOptions};
use zero_transport::outbound_stack::{connect_socket_transport_stack, StreamTransportStack};
use zero_transport::{quic, split_http, tls, RuntimeError};

use super::super::profile::VlessQuicClientProfile;
use super::{
    VlessDirectTransportRequest, VlessOutboundTransportRequest, VlessTransportOptions,
    VlessUdpOutboundTransportRequest,
};

pub(super) async fn open_vless_quic_transport(
    server: &str,
    port: u16,
    quic_config: &VlessQuicClientProfile,
) -> Result<TcpRelayStream, RuntimeError> {
    let server_name = quic_config.server_name.as_deref().unwrap_or(server);
    let alpn_protocols = quic_config.alpn_protocols();
    Ok(TcpRelayStream::new(
        quic::connect_quic(
            server,
            port,
            server_name,
            quic_config.insecure,
            &alpn_protocols,
        )
        .await?,
    ))
}

pub(super) async fn build_vless_direct_outbound_transport(
    request: VlessDirectTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VlessDirectTransportRequest {
        socket,
        options,
        quic,
        server,
        port,
    } = request;

    if let Some(quic_config) = quic {
        return open_vless_quic_transport(server, port, quic_config).await;
    }

    let socket = socket.ok_or_else(|| {
        RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing tcp socket for direct VLESS outbound transport",
        ))
    })?;

    build_vless_outbound_transport(VlessOutboundTransportRequest {
        socket,
        options,
        server,
        port,
    })
    .await
}

/// Wrap a raw TCP socket with the configured VLESS transport layer.
///
/// Handles every valid combination of TLS, Reality, WebSocket, gRPC, and H2.
/// Pass `None` for transports that are not configured.
pub(super) async fn build_vless_outbound_transport(
    request: VlessOutboundTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VlessOutboundTransportRequest {
        socket,
        options,
        server,
        port,
    } = request;
    let VlessTransportOptions {
        tls: tls_config,
        reality,
        ws: ws_config,
        grpc: grpc_config,
        h2: h2_config,
        http_upgrade: http_upgrade_config,
        split_http: split_http_config,
        source_dir,
    } = options;

    // XHTTP is handled first because it is mutually exclusive with other transports.
    if let Some(cfg) = split_http_config {
        let mode = split_http::XhttpMode::parse(&cfg.mode);
        // stream-one (and `auto`): a single bidirectional connection — one TCP/TLS
        // socket carries both the chunked upload and the chunked download.
        if mode.is_single_connection() {
            let carrier: TcpRelayStream = match tls_config {
                Some(tls) => tls::connect_tls_upstream(socket, tls, source_dir, server).await?,
                None => TcpRelayStream::new(socket),
            };
            return Ok(TcpRelayStream::new(
                split_http::connect_xhttp_stream_one(carrier, cfg).await?,
            ));
        }

        // packet-up / stream-up: legacy two-connection model (POST + GET).
        let peer = socket.peer_addr().map_err(RuntimeError::Io)?;
        let stream: TcpRelayStream = match tls_config {
            Some(tls) => {
                let post_stream =
                    tls::connect_tls_upstream(socket, tls, source_dir, server).await?;
                match TokioSocket::connect_addr(peer).await {
                    Ok(get_socket) => {
                        let get_stream =
                            match tls::connect_tls_upstream(get_socket, tls, source_dir, server)
                                .await
                            {
                                Ok(s) => s,
                                Err(e) => {
                                    // GET TLS connect failed; drop the POST stream.
                                    drop(post_stream);
                                    return Err(e);
                                }
                            };
                        TcpRelayStream::new(
                            split_http::connect_split_http(post_stream, get_stream, cfg).await?,
                        )
                    }
                    Err(e) => {
                        // Cannot open GET TCP; release the POST stream.
                        drop(post_stream);
                        return Err(RuntimeError::Io(io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            format!("split-http: failed to open GET connection: {e}"),
                        )));
                    }
                }
            }
            None => {
                let get_socket = TokioSocket::connect_addr(peer).await.map_err(|e| {
                    RuntimeError::Io(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        format!("split-http: failed to open GET connection: {e}"),
                    ))
                })?;
                TcpRelayStream::new(split_http::connect_split_http(socket, get_socket, cfg).await?)
            }
        };
        return Ok(stream);
    }

    if let Some(reality) = reality {
        return match (
            tls_config,
            ws_config,
            grpc_config,
            h2_config,
            http_upgrade_config,
        ) {
            (None, None, None, None, None) => {
                let server_name = reality.server_name.as_deref().unwrap_or(server);
                let reality_stream = upgrade_reality_client(
                    socket,
                    RealityClientOptions {
                        public_key: &reality.public_key,
                        short_id: &reality.short_id,
                        server_name,
                        cipher_suites: &reality.cipher_suites,
                        client_fingerprint: &reality.client_fingerprint,
                    },
                )
                .await?;
                let control = reality_stream.transport_bypass_control();
                Ok(TcpRelayStream::with_transport_bypass_control(
                    reality_stream,
                    control,
                ))
            }
            _ => Err(RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid vless outbound transport combination",
            ))),
        };
    }

    connect_socket_transport_stack(
        socket,
        StreamTransportStack {
            tls: tls_config,
            ws: ws_config,
            grpc: grpc_config,
            h2: h2_config,
            http_upgrade: http_upgrade_config,
            source_dir,
        },
        server,
        port,
        "invalid vless outbound transport combination",
    )
    .await
}

pub(super) async fn build_vless_udp_outbound_transport(
    request: VlessUdpOutboundTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VlessUdpOutboundTransportRequest {
        socket,
        options,
        server,
        port,
    } = request;

    if let Some(quic_config) = options.quic {
        return open_vless_quic_transport(server, port, quic_config).await;
    }

    build_vless_outbound_transport(VlessOutboundTransportRequest {
        socket,
        options: options.stream_options(),
        server,
        port,
    })
    .await
}
