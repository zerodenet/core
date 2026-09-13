//! VLESS only selects its TLS/REALITY carrier; XHTTP owns HTTP wire behavior.
use super::VlessTransportOptions;
use crate::reality::{upgrade_reality_client, RealityClientOptions};
use zero_platform_tokio::TcpRelayStream;
use zero_transport::{split_http, tls, RuntimeError};

pub(super) async fn carrier(
    stream: TcpRelayStream,
    options: VlessTransportOptions<'_>,
    server: &str,
    http2: bool,
) -> Result<TcpRelayStream, RuntimeError> {
    if let Some(reality) = options.reality {
        let stream = upgrade_reality_client(
            stream,
            RealityClientOptions {
                spider_x: &reality.spider_x,
                hybrid_key_exchange: reality.hybrid_key_exchange,
                mldsa65_verify: reality.mldsa65_verify.as_deref(),
                public_key: &reality.public_key,
                short_id: &reality.short_id,
                server_name: reality.server_name.as_deref().unwrap_or(server),
                cipher_suites: &reality.cipher_suites,
                client_fingerprint: &reality.client_fingerprint,
            },
        )
        .await?;
        let control = stream.transport_bypass_control();
        return Ok(TcpRelayStream::with_transport_bypass_control(
            stream, control,
        ));
    }
    match options.tls {
        Some(tls) => {
            let mut profile = tls.clone();
            // Negotiate the HTTP version selected by the XHTTP request executor.
            profile.alpn = vec![if http2 { "h2" } else { "http/1.1" }.into()];
            tls::connect_tls_stream(stream, &profile, options.source_dir, server).await
        }
        None => Ok(stream),
    }
}

pub(super) fn mode(options: VlessTransportOptions<'_>) -> split_http::XhttpMode {
    options
        .split_http
        .map(|cfg| split_http::XhttpMode::parse(&cfg.mode))
        .unwrap_or(split_http::XhttpMode::Auto)
        .resolve(options.reality.is_some())
}
