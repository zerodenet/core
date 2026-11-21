use std::sync::Arc;
use zero_core::{
    request::{Destination, ServicePurpose},
    Address,
};
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
pub(super) async fn exchange(
    proxy: &crate::Proxy,
    tag: &str,
    server: tokio::task::JoinHandle<()>,
    prefix: tokio::task::JoinHandle<()>,
) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let config = proxy.engine().config();
    let actual = proxy.clone();
    let tag = tag.to_owned();
    let end = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let host = proxy.protocols.runtime_host();
    let work = host.run_probe(
        &config,
        None,
        zero_runtime::reserve_probe().await.unwrap(),
        ServicePurpose::DiagnosticProbe,
        end,
        move |call| async move {
            let original = call.witness().unwrap();
            let services = actual.tcp_runtime_services().with_service_invocation(
                crate::component::direct_bridge::ServiceInvocation {
                    deadline: call.deadline(),
                    purpose: ServicePurpose::DiagnosticProbe,
                    caller: Some(call),
                },
            );
            let (resolved, _) = actual
                .engine()
                .resolve_route_decision(zero_engine::RouteDecision::Route(tag))
                .unwrap();
            assert!(matches!(
                services
                    .prepare_tcp_outbound(&resolved, zero_engine::RouteMode::Flow)
                    .unwrap_or_else(|failure| panic!("{}", failure.error)),
                crate::inventory::PreparedTcpOutbound::NativeRelay(_)
            ));
            let target = destination();
            let session = zero_core::Session::new(
                0,
                target.address,
                target.port,
                zero_core::Network::Tcp,
                zero_core::ProtocolType::UNKNOWN,
            );
            let connected = crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
                services,
                &session,
                resolved,
                zero_engine::RouteMode::Flow,
                crate::runtime::tcp_dispatch::TcpDispatchIntent::DiagnosticProbe,
            )
            .await
            .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
            let mut connected = crate::transport::extract_tcp_stream(connected).unwrap();
            connected
                .upstream
                .write_all(b"HEAD /native HTTP/1.1\r\nHost: native.example\r\n\r\n")
                .await
                .unwrap();
            connected.upstream.flush().await.unwrap();
            connected.upstream.shutdown().await.unwrap();
            let mut response = Vec::new();
            connected.upstream.read_to_end(&mut response).await.unwrap();
            // Both real peer directions finish before the selected view returns.
            server.await.unwrap();
            prefix.await.unwrap();
            drop(connected);
            assert!(actual.engine().active_sessions().is_empty());
            (response, original)
        },
    );
    let (response, original) = proxy
        .protocols
        .drive_component_supervision(work)
        .await
        .unwrap();
    let receipt = original.try_joined().unwrap();
    assert!(receipt.registration_released());
    receipt.result().unwrap();
    response
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
