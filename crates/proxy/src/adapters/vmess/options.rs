//! Cold protocol profile projection for the actual standard factories.
use super::*;
pub(super) fn outbound_options<'a>(
    tag: &'a str,
    endpoint: (&'a str, u16),
    protocol: &'a OutboundProtocolConfig,
) -> Option<
    VmessOutboundBuildOptionsRef<
        'a,
        zero_config::ClientTlsConfig,
        zero_config::WebSocketConfig,
        zero_config::GrpcConfig,
    >,
> {
    let OutboundProtocolConfig::Vmess {
        id,
        cipher,
        mux_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog_frames,
        mux_response_backlog_bytes,
        tls,
        ws,
        grpc,
        ..
    } = protocol
    else {
        return None;
    };
    Some(VmessOutboundBuildOptionsRef {
        tag,
        server: endpoint.0,
        port: endpoint.1,
        protocol: VmessOutboundOptionsRef {
            id,
            cipher,
            mux_concurrency: *mux_concurrency,
            mux_idle_timeout_secs: *mux_idle_timeout_secs,
            mux_response_backlog_frames: *mux_response_backlog_frames,
            mux_response_backlog_bytes: *mux_response_backlog_bytes,
        },
        tls: tls.as_deref(),
        ws: ws.as_deref(),
        grpc: grpc.as_deref(),
    })
}
