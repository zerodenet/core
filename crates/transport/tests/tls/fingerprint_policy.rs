use crate::profile::OwnedClientTlsProfile;
fn profile() -> OwnedClientTlsProfile {
    let mut options = zero_traits::ClientTlsOptions::default();
    options.parameters.min_version = "1.3".into();
    OwnedClientTlsProfile {
        options,
        server_name: Some("example.com".into()),
        disable_sni: true,
        ca_cert_path: None,
        insecure: false,
        alpn: vec!["h2".into()],
        client_fingerprint: Some("firefox-148".into()),
    }
}
#[test]
fn configured_groups_keep_custom_client_hello_and_real_key_material() {
    for (name, id) in [
        ("X25519", 29),
        ("P256", 23),
        ("P384", 24),
        ("P521", 25),
        ("X25519MLKEM768", 4588),
        ("SecP256r1MLKEM768", 4587),
        ("SecP384r1MLKEM1024", 4589),
    ] {
        let mut profile = profile();
        profile.options.parameters.curve_preferences = vec![name.into()];
        let config = super::super::config::client(&profile, None, false).unwrap();
        assert!(config.client_hello_profile.is_some());
        let mut client = rustls::ClientConnection::new(
            std::sync::Arc::new(config),
            "example.com".try_into().unwrap(),
        )
        .unwrap();
        let mut wire = Vec::new();
        client.write_tls(&mut wire).unwrap();
        let (_, extensions) = ztls::fingerprint::wire::parts(&wire[5..]).unwrap();
        assert!(!extensions.iter().any(|(k, _)| *k == 0));
        let share = &extensions.iter().find(|(k, _)| *k == 51).unwrap().1;
        assert_eq!(u16::from_be_bytes([share[2], share[3]]), id);
        assert!(share.len() > 6);
    }
}
#[test]
fn default_versions_and_resumption_retain_the_fingerprint() {
    let mut profile = profile();
    profile.options.parameters.min_version.clear();
    for resuming in [false, true] {
        profile.options.parameters.enable_session_resumption = resuming;
        let config = super::super::config::client(&profile, None, false).unwrap();
        assert!(config.client_hello_profile.is_some());
        let mut client = rustls::ClientConnection::new(
            std::sync::Arc::new(config),
            "example.com".try_into().unwrap(),
        )
        .unwrap();
        let mut wire = Vec::new();
        client.write_tls(&mut wire).unwrap();
        let (_, extensions) = ztls::fingerprint::wire::parts(&wire[5..]).unwrap();
        let versions = &extensions.iter().find(|(id, _)| *id == 43).unwrap().1;
        assert!(versions[1..].chunks_exact(2).any(|v| v == [3, 3]));
        assert!(versions[1..].chunks_exact(2).any(|v| v == [3, 4]));
    }
}
