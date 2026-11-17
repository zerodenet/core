//! Cold protocol profile projection for the actual standard factories.
use super::*;
pub(super) fn outbound_options<'a>(
    tag: &'a str,
    endpoint: (&'a str, u16),
    protocol: &'a OutboundProtocolConfig,
) -> Option<TrojanOutboundBuildOptionsRef<'a, zero_config::WebSocketConfig, zero_config::GrpcConfig>>
{
    let OutboundProtocolConfig::Trojan {
        password,
        sni,
        insecure,
        client_fingerprint,
        mux_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog_frames,
        mux_response_backlog_bytes,
        ws,
        grpc,
        ..
    } = protocol
    else {
        return None;
    };
    Some(TrojanOutboundBuildOptionsRef {
        tag,
        server: endpoint.0,
        port: endpoint.1,
        protocol: TrojanOutboundOptionsRef {
            password,
            sni: sni.as_deref(),
            insecure: *insecure,
            client_fingerprint: client_fingerprint.as_deref(),
            mux_concurrency: *mux_concurrency,
            mux_idle_timeout_secs: *mux_idle_timeout_secs,
            mux_response_backlog_frames: *mux_response_backlog_frames,
            mux_response_backlog_bytes: *mux_response_backlog_bytes,
        },
        ws: ws.as_deref(),
        grpc: grpc.as_deref(),
    })
}
