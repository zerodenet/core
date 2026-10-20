use super::*;
use zero_transport::profile::{OwnedH2Profile, OwnedHttpUpgradeProfile, OwnedSplitHttpProfile};
fn options(
    tls: &OwnedClientTlsProfile,
    ws: Option<&OwnedWebSocketProfile>,
    grpc: Option<&OwnedGrpcProfile>,
) -> Options {
    Options::from_options_refs(
        VlessOutboundBuildOptionsRef {
            tag: "profile",
            server: "localhost",
            port: 443,
            final_mask: None,
            mkcp: None,
            hysteria: None,
            download: None,
            protocol: crate::transport::VlessOutboundOptionsRef {
                encryption: None,
                id: "test-profile",
                flow: None,
                testpre: 0,
                testseed: &[],
                mux_concurrency: None,
                xudp_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
                reality: None,
                quic: None,
            },
            tls: Some(tls),
            ws,
            grpc,
            h2: None::<&OwnedH2Profile>,
            http_upgrade: None::<&OwnedHttpUpgradeProfile>,
            split_http: None::<&OwnedSplitHttpProfile>,
        },
        Default::default(),
    )
    .unwrap()
}
#[test]
fn actual_native_options_use_the_original_vless_carrier_alpn_defaults_and_keep_explicit_values() {
    let mut tls = OwnedClientTlsProfile {
        options: Default::default(),
        server_name: None,
        disable_sni: false,
        ca_cert_path: None,
        insecure: false,
        alpn: vec![],
        client_fingerprint: None,
    };
    let ws = OwnedWebSocketProfile {
        accept_proxy_protocol: false,
        heartbeat_period_secs: 0,
        host: None,
        path: "/".into(),
        headers: vec![],
    };
    let grpc = OwnedGrpcProfile {
        service_names: vec!["vless/Tun".into()],
        ..Default::default()
    };
    assert_eq!(
        options(&tls, None, None).tls.unwrap().alpn,
        ["h2", "http/1.1"]
    );
    assert_eq!(
        options(&tls, Some(&ws), None).tls.unwrap().alpn,
        ["http/1.1"]
    );
    assert_eq!(options(&tls, None, Some(&grpc)).tls.unwrap().alpn, ["h2"]);
    tls.alpn = vec!["explicit".into()];
    for (ws, grpc) in [(None, None), (Some(&ws), None), (None, Some(&grpc))] {
        assert_eq!(options(&tls, ws, grpc).tls.unwrap().alpn, tls.alpn);
    }
}
