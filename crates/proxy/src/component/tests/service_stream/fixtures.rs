use std::sync::Arc;
use zero_core::{
    request::{Destination, ServicePurpose},
    Address,
};
use zero_platform_tokio::contract::{network::CarrierContext, CancellationToken};
#[cfg(feature = "trojan")]
pub(super) const PASSWORD: &str = "native-password";
#[cfg(any(feature = "vmess", feature = "vless"))]
pub(super) const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("native.example".into()), 443)
}
pub(super) fn acceptor() -> tokio_rustls::TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        cert.signing_key.serialize_der(),
    ));
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key)
        .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}
#[cfg(feature = "trojan")]
pub(super) fn request() -> Vec<u8> {
    let mut bytes =
        b"e88960794334c8fb553896495e88d9d8f1bebc5b4d35c24628b0c30f\r\n\x01\x03\x0enative.example"
            .to_vec();
    bytes.extend_from_slice(&443u16.to_be_bytes());
    bytes.extend_from_slice(b"\r\n");
    bytes
}
#[cfg(feature = "vmess")]
pub(super) fn profile(cipher: &str) -> vmess::inbound::VmessInboundProfile {
    vmess::inbound::VmessInboundProfile::from_users(vec![vmess::inbound::VmessUser::from_config(
        ID, cipher, None, None, None, None, None, None,
    )
    .unwrap()])
}
pub(super) async fn connect(
    proxy: &crate::Proxy,
    tag: &str,
) -> zero_platform_tokio::TcpRelayStream {
    let services = proxy.tcp_runtime_services();
    let target = services.snapshot().plan().target_id(tag).unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    let plan =
        crate::runtime::tcp_dispatch::service::prepare(services.protocols(), &services, &resolved)
            .unwrap_or_else(|failure| {
                panic!("prepare Service at {}: {}", failure.stage, failure.error)
            })
            .expect("whole selected relay must be native");
    crate::runtime::tcp_dispatch::service::connect(
        services.execution(),
        destination(),
        plan,
        ServicePurpose::DiagnosticProbe,
        CarrierContext {
            session: None,
            deadline: Some(tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
            cancel: CancellationToken::new(),
        },
    )
    .await
    .unwrap()
}

#[cfg(feature = "vless")]
pub(super) fn vless_request() -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend_from_slice(&[
        0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55, 0x55,
        0x55,
    ]);
    bytes.extend_from_slice(&[0, 1, 1, 0xbb, 2, 14]);
    bytes.extend_from_slice(b"native.example");
    bytes
}

#[cfg(feature = "vless")]
pub(super) fn vless_vision_request() -> Vec<u8> {
    let plain = vless_request();
    let mut bytes = plain[..17].to_vec();
    bytes.extend_from_slice(&[18, 0x0a, 16]);
    bytes.extend_from_slice(b"xtls-rprx-vision");
    bytes.extend_from_slice(&plain[18..]);
    bytes
}
