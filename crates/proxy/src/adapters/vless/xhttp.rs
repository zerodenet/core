//! Thin projection of native configuration into protocol-owned carrier plans.
use vless::transport::{
    VlessQuicClientOptionsRef, VlessRealityClientOptionsRef, VlessXhttpDownloadOptionsRef,
};
pub(super) fn download_options(
    config: &zero_config::SplitHttpDownloadConfig,
) -> VlessXhttpDownloadOptionsRef<'_, zero_config::ClientTlsConfig, zero_config::SplitHttpConfig> {
    VlessXhttpDownloadOptionsRef {
        server: &config.server,
        port: config.port,
        tls: config.tls.as_ref(),
        split_http: &config.split_http,
        reality: config
            .reality
            .as_ref()
            .map(|r| VlessRealityClientOptionsRef {
                spider_x: &r.spider_x,
                hybrid_key_exchange: r.hybrid_key_exchange,
                mldsa65_verify: r.mldsa65_verify.as_deref(),
                public_key: &r.public_key,
                short_id: &r.short_id,
                server_name: r.server_name.as_deref(),
                cipher_suites: &r.cipher_suites,
                client_fingerprint: &r.client_fingerprint,
            }),
        quic: config.quic.as_ref().map(|q| VlessQuicClientOptionsRef {
            tls: q,
            server_name: q.server_name.as_deref(),
            insecure: q.insecure,
            ca_cert_path: q.ca_cert_path.as_deref(),
        }),
    }
}
