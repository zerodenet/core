use super::*;
use std::{num::NonZeroUsize, path::Path, time::Duration};
use zero_platform_tokio::contract::component::{ComponentFactory, ResourceDeclaration, RoleKind};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
fn options(
    runtime: &crate::transport::VmessTransportRuntime,
    directory: &str,
    bound: Option<NonZeroUsize>,
) -> Result<Options, zero_core::Error> {
    let tls = OwnedClientTlsProfile {
        ca_cert_path: Some("../authority.pem".into()),
        server_name: Some("localhost".into()),
        alpn: vec!["h2".into()],
        options: Default::default(),
        disable_sni: false,
        insecure: false,
        client_fingerprint: None,
    };
    let grpc = OwnedGrpcProfile {
        service_names: vec!["ca/Tun".into()],
        ..Default::default()
    };
    let input: crate::transport::VmessOutboundBuildOptionsRef<
        '_,
        OwnedClientTlsProfile,
        OwnedWebSocketProfile,
        OwnedGrpcProfile,
    > = crate::transport::VmessOutboundBuildOptionsRef {
        tag: "node-ca",
        server: "127.0.0.1",
        port: 443,
        protocol: crate::transport::VmessOutboundOptionsRef {
            id: "11111111-2222-3333-4444-555555555555",
            cipher: "aes-128-gcm",
            mux_concurrency: None,
            mux_idle_timeout_secs: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
        },
        tls: Some(&tls),
        ws: None,
        grpc: Some(&grpc),
    };
    Options::from_node_options_refs_at(
        input,
        Default::default(),
        Some(Path::new(directory)),
        bound,
        NonZeroUsize::new(2).unwrap(),
        Duration::from_secs(5),
        runtime,
    )
}
#[test]
fn node_ca_preparation_keeps_file_grant_before_original_carrier_without_reading_material() {
    let runtime = crate::transport::VmessTransportRuntime::default();
    let prepared = options(&runtime, "/not-opened/config", NonZeroUsize::new(4096)).unwrap();
    let spec = Factory.describe(&prepared);
    assert_eq!(spec.roles, vec![RoleKind::Datagram]);
    assert!(
        matches!(&spec.resources[..], [ResourceDeclaration::File { path, max_bytes:4096 }, ResourceDeclaration::ControlledNodeStream { max_sockets, .. }] if path == Path::new("/not-opened/config/../authority.pem") && max_sockets.get()==2)
    );
    assert!(options(&runtime, "/not-opened/config", None).is_err());
    assert!(options(
        &runtime,
        "/not-opened/config",
        NonZeroUsize::new(usize::MAX)
    )
    .is_err());
}
#[test]
fn node_ca_identity_and_actual_state_follow_the_complete_file_policy() {
    let runtime = crate::transport::VmessTransportRuntime::default();
    let a = options(&runtime, "/not-opened/a", NonZeroUsize::new(4096)).unwrap();
    let same = options(&runtime, "/not-opened/a", NonZeroUsize::new(4096)).unwrap();
    assert_eq!(a.identity, same.identity);
    assert!(Arc::ptr_eq(&a.state, &same.state));
    for b in [
        options(&runtime, "/not-opened/b", NonZeroUsize::new(4096)).unwrap(),
        options(&runtime, "/not-opened/a", NonZeroUsize::new(4097)).unwrap(),
    ] {
        assert_ne!(a.identity, b.identity);
        assert!(!Arc::ptr_eq(&a.state, &b.state));
    }
}
