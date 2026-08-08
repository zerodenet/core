use std::io;

use zero_platform_tokio::{RelayCarrier, TcpRelayStream};
use zero_transport::RuntimeError;

use crate::reality::{upgrade_reality_client, RealityClientOptions};
use zero_transport::outbound_stack::{connect_relay_transport_stack, StreamTransportStack};
use zero_transport::{split_http, tls};

use super::{VlessFinalHopTransportRequest, VlessTransportOptions, VlessUdpTransportOptions};

pub(super) async fn build_vless_outbound_transport_over_stream(
    request: VlessFinalHopTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VlessFinalHopTransportRequest { carrier, options } = request;
    let RelayCarrier {
        stream,
        server,
        port,
    } = carrier;
    // Shadow to match the original &str / u16 types used throughout the function.
    let server: &str = &server;
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

    if let Some(cfg) = split_http_config {
        let mode = split_http::XhttpMode::parse(&cfg.mode);
        // stream-one (and `auto`): a single bidirectional connection. This is
        // the path that makes XHTTP usable as a relay-chain final hop 閳?the
        // relay prefix delivers exactly one stream, which stream-one uses for
        // both the chunked upload and the chunked download.
        if mode.is_single_connection() {
            let carrier: TcpRelayStream = match tls_config {
                Some(tls) => tls::connect_tls_stream(stream, tls, source_dir, server).await?,
                None => stream,
            };
            return Ok(TcpRelayStream::new(
                split_http::connect_xhttp_stream_one(carrier, cfg).await?,
            ));
        }
        // packet-up / stream-up require two independent connections (POST + GET);
        // a relay chain provides only one stream per hop, so they cannot serve
        // as a final hop here. The two-stream path lives in
        // `build_vless_split_http_over_relay` (used by the UDP relay fast path).
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "xhttp packet-up/stream-up require two streams; use mode stream-one (or auto) for relay final-hop",
        )));
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
                    stream,
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
            _ => Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid vless final-hop transport combination",
            ))),
        };
    }

    connect_relay_transport_stack(
        stream,
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
        "invalid vless final-hop transport combination",
    )
    .await
}

/// Build a SplitHTTP transport over a relay chain.
///
/// SplitHTTP uses separate POST (write) and GET (read) TCP channels.
/// This function receives two independent streams 閳?each already tunneled
/// through the same relay prefix hops 閳?and pairs them via
/// [`split_http::connect_split_http`].
pub(super) async fn build_vless_split_http_over_relay(
    post_stream: TcpRelayStream,
    get_stream: TcpRelayStream,
    options: VlessUdpTransportOptions<'_>,
    server: &str,
) -> Result<TcpRelayStream, RuntimeError> {
    let VlessUdpTransportOptions {
        tls: tls_config,
        split_http: split_http_config,
        source_dir,
        ..
    } = options;
    let config = split_http_config.ok_or_else(|| {
        RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "split-http relay transport requires split_http config",
        ))
    })?;

    if split_http::XhttpMode::parse(&config.mode).is_single_connection() {
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "split-http relay transport requires packet-up or stream-up mode",
        )));
    }

    let post_stream = match tls_config {
        Some(tls) => tls::connect_tls_stream(post_stream, tls, source_dir, server).await?,
        None => post_stream,
    };
    let get_stream = match tls_config {
        Some(tls) => tls::connect_tls_stream(get_stream, tls, source_dir, server).await?,
        None => get_stream,
    };
    let paired = split_http::connect_split_http(post_stream, get_stream, config).await?;
    Ok(TcpRelayStream::new(paired))
}
