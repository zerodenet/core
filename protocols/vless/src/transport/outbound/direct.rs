use std::io;

use zero_platform_tokio::{TcpRelayStream, TokioSocket};

use crate::reality::{upgrade_reality_client, RealityClientOptions};
use zero_transport::outbound_stack::{connect_socket_transport_stack, StreamTransportStack};
use zero_transport::{quic, split_http, RuntimeError};

use super::super::profile::VlessQuicClientProfile;
use super::{
    VlessDirectTransportRequest, VlessOutboundTransportRequest, VlessTransportOptions,
    VlessUdpOutboundTransportRequest,
};

pub(super) async fn open_vless_quic_transport(
    server: &str,
    port: u16,
    quic_config: &VlessQuicClientProfile,
    xhttp: Option<&zero_transport::profile::OwnedSplitHttpProfile>,
    source_dir: Option<&std::path::Path>,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<TcpRelayStream, RuntimeError> {
    let server_name = quic_config.server_name.as_deref().unwrap_or(server);
    let alpn_protocols = if xhttp.is_some() {
        vec![b"h3".to_vec()]
    } else {
        quic_config.alpn_protocols()
    };
    let ca_path = quic_config.ca_cert_path.as_deref().map(|path| {
        let path = std::path::Path::new(path);
        source_dir.map_or_else(|| path.to_path_buf(), |base| base.join(path))
    });
    let config = quic::client_config_with_options(
        quic_config.insecure,
        None,
        &alpn_protocols,
        None,
        ca_path.as_deref(),
        &quic_config.tls_options,
    )?;
    if let Some(profile) = xhttp {
        let connection =
            quic::connect_quic_endpoint(server, port, server_name, config, sockets).await?;
        return Ok(TcpRelayStream::new(
            split_http::connect_xhttp_h3(connection, profile).await?,
        ));
    }
    Ok(TcpRelayStream::new(
        quic::connect_quic_with_config(server, port, server_name, config, sockets).await?,
    ))
}

pub(super) async fn build_vless_direct_outbound_transport(
    request: VlessDirectTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VlessDirectTransportRequest {
        socket,
        options,
        quic,
        socket_factory,
        server,
        port,
    } = request;

    if let Some(quic_config) = quic {
        return open_vless_quic_transport(
            server,
            port,
            quic_config,
            options.split_http,
            options.source_dir,
            &socket_factory,
        )
        .await;
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
    let companion_egress = socket.egress_interface().cloned();

    if let Some(cfg) = split_http_config {
        let mode = super::xhttp::mode(options);
        let single = mode.is_single_connection();
        let http2 = single || mode == split_http::XhttpMode::StreamUp;
        let peer = socket.peer_addr().map_err(RuntimeError::Io)?;
        let post =
            super::xhttp::carrier(TcpRelayStream::new(socket), options, server, http2).await?;
        if single {
            return Ok(TcpRelayStream::new(
                split_http::connect_xhttp_stream_one(post, cfg).await?,
            ));
        }
        let get = TokioSocket::connect_addr_on(peer, companion_egress.as_ref()).await?;
        let get = super::xhttp::carrier(TcpRelayStream::new(get), options, server, http2).await?;
        return Ok(TcpRelayStream::new(
            split_http::connect_split_http(post, get, cfg).await?,
        ));
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
                        spider_x: &reality.spider_x,
                        hybrid_key_exchange: reality.hybrid_key_exchange,
                        mldsa65_verify: reality.mldsa65_verify.as_deref(),
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
        socket_factory,
        server,
        port,
    } = request;

    if let Some(quic_config) = options.quic {
        return open_vless_quic_transport(
            server,
            port,
            quic_config,
            options.split_http,
            options.source_dir,
            &socket_factory,
        )
        .await;
    }

    build_vless_outbound_transport(VlessOutboundTransportRequest {
        socket,
        options: options.stream_options(),
        server,
        port,
    })
    .await
}

pub(super) async fn open_vless_xhttp_quic(
    server: &str,
    port: u16,
    profile: &VlessQuicClientProfile,
    source_dir: Option<&std::path::Path>,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
    keepalive: i64,
) -> Result<quic::QuicConnection, RuntimeError> {
    let ca = profile.ca_cert_path.as_deref().map(|path| {
        source_dir.map_or_else(|| std::path::PathBuf::from(path), |base| base.join(path))
    });
    let mut config = quic::client_config_with_options(
        profile.insecure,
        None,
        &[b"h3".to_vec()],
        None,
        ca.as_deref(),
        &profile.tls_options,
    )?;
    quic::set_http_keepalive(&mut config, keepalive);
    quic::connect_quic_endpoint(
        server,
        port,
        profile.server_name.as_deref().unwrap_or(server),
        config,
        sockets,
    )
    .await
}
