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
        assert!(versions[1..].as_chunks::<2>().0.contains(&[3, 3]));
        assert!(versions[1..].as_chunks::<2>().0.contains(&[3, 4]));
    }
}

#[test]
fn all_pinned_fingerprint_cipher_suites_have_real_negotiation_implementations() {
    for preset in ztls::fingerprint::ClientHelloProfile::VERSIONED {
        let mut profile = profile();
        profile.client_fingerprint = Some(preset.canonical_name().into());
        profile.options.parameters.min_version.clear();
        let config = super::super::config::client(&profile, None, false).unwrap();
        let mut client = rustls::ClientConnection::new(
            std::sync::Arc::new(config),
            "example.com".try_into().unwrap(),
        )
        .unwrap();
        let mut wire = Vec::new();
        client.write_tls(&mut wire).unwrap();
        let (actual, _) = ztls::fingerprint::wire::parts(&wire[5..]).unwrap();
        let (expected, _) = ztls::fingerprint::wire::preset_parts(*preset).unwrap();
        for id in expected
            .into_iter()
            .filter(|id| !ztls::fingerprint::wire::is_grease(*id))
        {
            assert!(
                actual.contains(&id),
                "{} omitted cipher {id:#06x}",
                preset.canonical_name()
            );
        }
    }
}

#[test]
fn explicit_cbc_and_rsa_keep_auto_backend_fingerprint_and_tls12() {
    for suite in [
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
    ] {
        let mut profile = profile();
        profile.options.parameters.min_version.clear();
        profile.options.parameters.cipher_suites = vec![suite.into()];
        assert!(!super::super::openssl::use_openssl_client(&profile).unwrap());
        let config = super::super::config::client(&profile, None, false).unwrap();
        assert!(config.client_hello_profile.is_some());
        let mut client = rustls::ClientConnection::new(
            std::sync::Arc::new(config),
            "example.com".try_into().unwrap(),
        )
        .unwrap();
        let mut wire = Vec::new();
        client.write_tls(&mut wire).unwrap();
        let (suites, _) = ztls::fingerprint::wire::parts(&wire[5..]).unwrap();
        assert!(suites.contains(&ztls::settings::cipher_suite(suite).unwrap()));
    }
}

#[test]
fn alps_is_not_offered_for_an_unconfigured_application_protocol_or_tls12_only() {
    for (alpn, max) in [("http/1.1", ""), ("h2", "1.2")] {
        let mut profile = profile();
        profile.client_fingerprint = Some("chrome".into());
        profile.alpn = vec![alpn.into()];
        profile.options.parameters.min_version.clear();
        profile.options.parameters.max_version = max.into();
        let config = super::super::config::client(&profile, None, false).unwrap();
        let mut client = rustls::ClientConnection::new(
            std::sync::Arc::new(config),
            "example.com".try_into().unwrap(),
        )
        .unwrap();
        let mut wire = Vec::new();
        client.write_tls(&mut wire).unwrap();
        let (_, extensions) = ztls::fingerprint::wire::parts(&wire[5..]).unwrap();
        assert!(!extensions.iter().any(|(id, _)| matches!(id, 17513 | 17613)));
    }
}
