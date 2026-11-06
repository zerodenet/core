//! Existing complete outbound profile projection, without runtime IO.
use super::*;
pub(super) fn outbound_options<'a>(
    tag: &'a str,
    endpoint: (&'a str, u16),
    protocol: &'a OutboundProtocolConfig,
) -> Option<
    VlessOutboundBuildOptionsRef<
        'a,
        zero_config::ClientTlsConfig,
        zero_config::WebSocketConfig,
        zero_config::GrpcConfig,
        zero_config::H2Config,
        zero_config::HttpUpgradeConfig,
        zero_config::SplitHttpConfig,
    >,
> {
    let OutboundProtocolConfig::Vless {
        final_mask,
        mkcp,
        hysteria,
        encryption,
        id,
        flow,
        testpre,
        testseed,
        mux_concurrency,
        xudp_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog_frames,
        mux_response_backlog_bytes,
        tls,
        reality,
        ws,
        grpc,
        h2,
        http_upgrade,
        split_http,
        quic,
        ..
    } = protocol
    else {
        return None;
    };
    Some(VlessOutboundBuildOptionsRef {
        final_mask: final_mask.as_deref().map(finalmask::options),
        mkcp: mkcp.as_deref().map(mkcp::settings),
        hysteria: hysteria.as_deref().map(hysteria::options),
        download: split_http
            .as_deref()
            .and_then(|cfg| cfg.download_settings.as_deref())
            .map(xhttp::download_options),
        tag,
        server: endpoint.0,
        port: endpoint.1,
        protocol: VlessOutboundOptionsRef {
            encryption: encryption.as_deref(),
            id,
            flow: flow.as_deref(),
            testpre: *testpre,
            testseed,
            mux_concurrency: *mux_concurrency,
            xudp_concurrency: *xudp_concurrency,
            mux_idle_timeout_secs: *mux_idle_timeout_secs,
            mux_response_backlog_frames: *mux_response_backlog_frames,
            mux_response_backlog_bytes: *mux_response_backlog_bytes,
            reality: reality
                .as_deref()
                .map(|reality| VlessRealityClientOptionsRef {
                    spider_x: &reality.spider_x,
                    hybrid_key_exchange: reality.hybrid_key_exchange,
                    mldsa65_verify: reality.mldsa65_verify.as_deref(),
                    public_key: reality.public_key.as_str(),
                    short_id: reality.short_id.as_str(),
                    server_name: reality.server_name.as_deref(),
                    cipher_suites: reality.cipher_suites.as_slice(),
                    client_fingerprint: reality.client_fingerprint.as_str(),
                }),
            quic: quic.as_deref().map(|quic| VlessQuicClientOptionsRef {
                tls: quic,
                server_name: quic.server_name.as_deref(),
                insecure: quic.insecure,
                ca_cert_path: quic.ca_cert_path.as_deref(),
            }),
        },
        tls: tls.as_deref(),
        ws: ws.as_deref(),
        grpc: grpc.as_deref(),
        h2: h2.as_deref(),
        http_upgrade: http_upgrade.as_deref(),
        split_http: split_http.as_deref(),
    })
}
