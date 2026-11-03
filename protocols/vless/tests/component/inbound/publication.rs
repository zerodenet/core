use super::*;
#[tokio::test]
async fn standard_cold_preparation_does_not_publish_credentials_and_activation_keeps_the_shared_profile(
) {
    let runtime = crate::transport::VlessTransportRuntime::default();
    let current = request(USER, false)
        .prepare_publication(&runtime, "profile")
        .activate();
    let candidate = request("11111111-2222-3333-4444-555555555555", false)
        .prepare_publication(&runtime, "profile");
    assert_eq!(current.profile.user_count(), 1);
    let (mut writer, reader) = tokio::io::duplex(256);
    writer.write_all(&header(1)).await.unwrap();
    let mut reader = TcpRelayStream::new(reader);
    assert!(current
        .profile
        .accept_standard_facts(&mut reader)
        .await
        .is_ok());
    let active = candidate.activate();
    let (mut writer, reader) = tokio::io::duplex(256);
    writer.write_all(&header(1)).await.unwrap();
    let mut reader = TcpRelayStream::new(reader);
    assert!(
        current
            .profile
            .accept_standard_facts(&mut reader)
            .await
            .is_err(),
        "all current clones use the same replaced complete users"
    );
    assert_eq!(active.profile.user_count(), 1);
}

#[test]
fn standard_fallback_remains_cold_before_credential_publication() {
    let runtime = crate::transport::VlessTransportRuntime::default();
    let current = request(USER, false)
        .prepare_publication(&runtime, "profile")
        .activate();
    let mut request = request("11111111-2222-3333-4444-555555555555", false);
    request.fallback_enabled = true;
    let endpoint = zero_traits::FallbackEndpoint::Tcp {
        server: "127.0.0.1".into(),
        port: 8080,
    };
    request.transport.fallback_policy =
        crate::fallback::FallbackPolicy::new(vec![zero_traits::FallbackRule {
            name: String::new(),
            alpn: String::new(),
            path: String::new(),
            endpoint: endpoint.clone(),
            proxy_protocol: 0,
        }]);
    let candidate = request.prepare_publication(&runtime, "profile");
    candidate.validate_standard_source().unwrap();
    assert_eq!(candidate.target_endpoints(), vec![endpoint]);
    // Cold validation never commits candidate users or opens a native owner.
    assert_eq!(current.profile.user_count(), 1);
}

#[test]
fn standard_quic_projects_owned_appearance_before_credential_publication() {
    let runtime = crate::transport::VlessTransportRuntime::default();
    let current = request(USER, false)
        .prepare_publication(&runtime, "profile")
        .activate();
    let candidate = request("11111111-2222-3333-4444-555555555555", false)
        .with_hysteria(Some(
            zero_transport::hysteria::Profile::new("secret")
                .unwrap()
                .with_masquerade(
                    zero_transport::hysteria::Masquerade::proxy("http://127.0.0.1:9", false, false)
                        .unwrap(),
                ),
        ))
        .prepare_publication(&runtime, "profile");
    candidate.validate_standard_quic_source().unwrap();
    assert_eq!(
        candidate.target_endpoints(),
        vec![zero_traits::FallbackEndpoint::Tcp {
            server: "127.0.0.1".into(),
            port: 9,
        }]
    );
    // The integrated source declares this exact endpoint for scoped origin I/O;
    // cold projection must still leave the live credential profile untouched.
    assert_eq!(current.profile.user_count(), 1);
}
