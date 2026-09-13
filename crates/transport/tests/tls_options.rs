#![cfg(feature = "tls")]
use base64::Engine;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_transport::{
    profile::{OwnedClientTlsProfile, OwnedServerTlsProfile},
    tls,
};
struct Material {
    dir: PathBuf,
    leaf: Vec<u8>,
    ca: Vec<u8>,
}
impl Material {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("zero-tls-options-{}", rand::random::<u64>()));
        std::fs::create_dir(&dir).unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let key = rcgen::KeyPair::generate().unwrap();
        let leaf = rcgen::CertificateParams::new(vec!["one.test".into()])
            .unwrap()
            .signed_by(&key, &issuer)
            .unwrap();
        std::fs::write(dir.join("one.pem"), leaf.pem() + &ca.pem()).unwrap();
        std::fs::write(dir.join("one.key"), key.serialize_pem()).unwrap();
        std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
        std::fs::write(dir.join("ca.key"), ca_key.serialize_pem()).unwrap();
        let second = rcgen::generate_simple_self_signed(vec!["two.test".into()]).unwrap();
        std::fs::write(dir.join("two.pem"), second.cert.pem()).unwrap();
        std::fs::write(dir.join("two.key"), second.signing_key.serialize_pem()).unwrap();
        Self {
            dir,
            leaf: leaf.der().to_vec(),
            ca: ca.der().to_vec(),
        }
    }
    fn server(&self) -> OwnedServerTlsProfile {
        OwnedServerTlsProfile {
            options: Default::default(),
            cert_path: "one.pem".into(),
            key_path: "one.key".into(),
            alpn: vec!["h2".into()],
            server_fingerprint: None,
        }
    }
    fn client(&self) -> OwnedClientTlsProfile {
        OwnedClientTlsProfile {
            options: zero_traits::ClientTlsOptions {
                disable_system_roots: true,
                ..Default::default()
            },
            server_name: None,
            disable_sni: false,
            ca_cert_path: Some("ca.pem".into()),
            insecure: false,
            alpn: vec!["h2".into()],
            client_fingerprint: None,
        }
    }
}
impl Drop for Material {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
fn pin(der: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(ring::digest::digest(&ring::digest::SHA256, der))
}
async fn exchange(
    client: rustls::ClientConfig,
    server: Arc<rustls::ServerConfig>,
    name: &str,
) -> Result<(rustls::ProtocolVersion, rustls::HandshakeKind, Vec<u8>), String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
        let acceptor = tokio_rustls::TlsAcceptor::from(server);
        let (client, server) = tokio::join!(
            connector.connect(name.to_owned().try_into().unwrap(), a),
            acceptor.accept(b)
        );
        let mut client = client.map_err(|e| e.to_string())?;
        let mut server = server.map_err(|e| e.to_string())?;
        server.write_all(b"x").await.map_err(|e| e.to_string())?;
        let mut data = [0];
        client
            .read_exact(&mut data)
            .await
            .map_err(|e| e.to_string())?;
        let connection = client.get_ref().1;
        Ok((
            connection.protocol_version().unwrap(),
            connection.handshake_kind().unwrap(),
            connection.peer_certificates().unwrap()[0].to_vec(),
        ))
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn exchange_chain(
    client: rustls::ClientConfig,
    server: Arc<rustls::ServerConfig>,
    name: &str,
) -> Result<Vec<Vec<u8>>, String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
        let acceptor = tokio_rustls::TlsAcceptor::from(server);
        let (client, server) = tokio::join!(
            connector.connect(name.to_owned().try_into().unwrap(), a),
            acceptor.accept(b)
        );
        let client = client.map_err(|e| e.to_string())?;
        server.map_err(|e| e.to_string())?;
        Ok(client
            .get_ref()
            .1
            .peer_certificates()
            .unwrap()
            .iter()
            .map(|certificate| certificate.to_vec())
            .collect())
    })
    .await
    .map_err(|e| e.to_string())?
}
#[tokio::test]
async fn pinned_leaf_pinned_ca_and_alternate_names_keep_distinct_verification_contracts() {
    let material = Material::new();
    let server =
        Arc::new(tls::build_server_config(&material.server(), Some(&material.dir), false).unwrap());
    for (leaf_pin, ca_pin, names, name, accepted) in [
        (false, false, vec![], "one.test", true),
        (false, false, vec![], "wrong.test", false),
        (true, false, vec![], "wrong.test", true),
        (false, true, vec![], "one.test", true),
        (false, true, vec![], "wrong.test", false),
        (false, true, vec!["one.test"], "front.test", true),
        (false, false, vec!["one.test"], "front.test", true),
    ] {
        let mut profile = material.client();
        if leaf_pin {
            profile
                .options
                .pinned_peer_cert_sha256
                .push(pin(&material.leaf));
            profile.ca_cert_path = None;
        }
        if ca_pin {
            profile
                .options
                .pinned_peer_cert_sha256
                .push(pin(&material.ca));
            profile.ca_cert_path = None;
        }
        profile.options.verify_peer_names = names.into_iter().map(str::to_owned).collect();
        let client = tls::config::client(&profile, Some(&material.dir), false).unwrap();
        assert_eq!(
            exchange(client, server.clone(), name).await.is_ok(),
            accepted,
            "leaf={leaf_pin} ca={ca_pin} name={name}"
        );
    }
    let mut profile = material.client();
    profile.insecure = true;
    profile.options.pinned_peer_cert_sha256.push(pin(b"wrong"));
    assert!(exchange(
        tls::config::client(&profile, Some(&material.dir), false).unwrap(),
        server,
        "one.test"
    )
    .await
    .is_err());
}
#[tokio::test]
async fn tls_versions_hybrid_groups_and_sni_certificate_selection_are_applied() {
    let material = Material::new();
    for (version, group) in [
        ("1.2", "P256"),
        ("1.2", "CurveP521"),
        ("1.3", "X25519MLKEM768"),
        ("1.3", "SecP256r1MLKEM768"),
        ("1.3", "SecP384r1MLKEM1024"),
    ] {
        let mut server = material.server();
        server.options.parameters.min_version = version.into();
        server.options.parameters.max_version = version.into();
        server.options.parameters.curve_preferences = vec![group.into()];
        server.options.reject_unknown_sni = true;
        server
            .options
            .certificates
            .push(zero_traits::TlsCertificateFiles {
                cert_path: "two.pem".into(),
                key_path: "two.key".into(),
                ocsp_path: None,
                ocsp_stapling_secs: 0,
                usage: zero_traits::TlsCertificateUsage::Encipherment,
                build_chain: false,
            });
        let config =
            Arc::new(tls::build_server_config(&server, Some(&material.dir), false).unwrap());
        let mut client = material.client();
        client.insecure = true;
        client.options.parameters = server.options.parameters.clone();
        let (selected, _, cert) = exchange(
            tls::config::client(&client, Some(&material.dir), false).unwrap(),
            config.clone(),
            "two.test",
        )
        .await
        .unwrap();
        assert_eq!(
            selected,
            if version == "1.2" {
                rustls::ProtocolVersion::TLSv1_2
            } else {
                rustls::ProtocolVersion::TLSv1_3
            }
        );
        assert_ne!(cert, material.leaf);
        assert!(exchange(
            tls::config::client(&client, Some(&material.dir), false).unwrap(),
            config,
            "unknown.test"
        )
        .await
        .is_err());
    }
}
#[tokio::test]
async fn resumption_is_opt_in_and_cached_only_with_the_same_verification_policy() {
    let material = Material::new();
    let mut server = material.server();
    server.options.parameters.enable_session_resumption = true;
    let server = Arc::new(tls::build_server_config(&server, Some(&material.dir), false).unwrap());
    let mut profile = material.client();
    profile.options.parameters.enable_session_resumption = true;
    for kind in [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed] {
        let client = tls::config::client(&profile, Some(&material.dir), false).unwrap();
        assert_eq!(
            exchange(client, server.clone(), "one.test")
                .await
                .unwrap()
                .1,
            kind
        );
    }
    profile.options.verify_peer_names = vec!["wrong.test".into()];
    assert!(exchange(
        tls::config::client(&profile, Some(&material.dir), false).unwrap(),
        server.clone(),
        "one.test"
    )
    .await
    .is_err());
    profile.options.verify_peer_names.clear();
    profile.options.parameters.enable_session_resumption = false;
    assert_eq!(
        exchange(
            tls::config::client(&profile, Some(&material.dir), false).unwrap(),
            server,
            "one.test"
        )
        .await
        .unwrap()
        .1,
        rustls::HandshakeKind::Full
    );
}

#[tokio::test]
async fn certificate_refresh_replaces_valid_files_and_preserves_the_last_good_key_pair() {
    let material = Material::new();
    let mut profile = material.server();
    profile.options.reload_interval_secs = 1;
    let server = Arc::new(tls::build_server_config(&profile, Some(&material.dir), false).unwrap());
    let mut client = material.client();
    client.insecure = true;
    let connect = || tls::config::client(&client, Some(&material.dir), false).unwrap();
    assert_eq!(
        exchange(connect(), server.clone(), "one.test")
            .await
            .unwrap()
            .2,
        material.leaf
    );
    std::fs::copy(material.dir.join("two.pem"), material.dir.join("one.pem")).unwrap();
    std::fs::copy(material.dir.join("two.key"), material.dir.join("one.key")).unwrap();
    let changed = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let received = exchange(connect(), server.clone(), "two.test")
                .await
                .unwrap()
                .2;
            if received != material.leaf {
                break received;
            }
        }
    })
    .await
    .expect("TLS certificate did not refresh");
    std::fs::write(material.dir.join("one.key"), "invalid key").unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        exchange(connect(), server, "two.test").await.unwrap().2,
        changed
    );
}

#[tokio::test]
async fn authority_issues_and_caches_per_sni_with_the_configured_chain() {
    let material = Material::new();
    let mut profile = material.server();
    profile.cert_path.clear();
    profile.key_path.clear();
    profile.options.reject_unknown_sni = true;
    profile
        .options
        .certificates
        .push(zero_traits::TlsCertificateFiles {
            cert_path: "ca.pem".into(),
            key_path: "ca.key".into(),
            ocsp_path: None,
            ocsp_stapling_secs: 0,
            usage: zero_traits::TlsCertificateUsage::AuthorityIssue,
            build_chain: true,
        });
    let server = Arc::new(tls::build_server_config(&profile, Some(&material.dir), false).unwrap());
    let connect = || tls::config::client(&material.client(), Some(&material.dir), false).unwrap();

    let first = exchange_chain(connect(), server.clone(), "issued.test")
        .await
        .unwrap();
    let cached = exchange_chain(connect(), server.clone(), "issued.test")
        .await
        .unwrap();
    let other = exchange_chain(connect(), server, "other.test")
        .await
        .unwrap();

    assert_eq!(first.len(), 2);
    assert_eq!(first[1], material.ca);
    assert_eq!(first[0], cached[0]);
    assert_ne!(first[0], other[0]);
    let (_, parsed) = x509_parser::parse_x509_certificate(&first[0]).unwrap();
    let san = parsed.subject_alternative_name().unwrap().unwrap();
    assert!(san.value.general_names.iter().any(|name| matches!(
        name,
        x509_parser::extensions::GeneralName::DNSName("issued.test")
    )));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let remaining = parsed.validity().not_after.timestamp() - now;
    assert!((120..=3600).contains(&remaining));
}

#[tokio::test]
async fn expiring_static_sni_certificate_is_replaced_by_authority_issuance() {
    let material = Material::new();
    let mut profile = material.server();
    let authority_key =
        rcgen::KeyPair::from_pem(&std::fs::read_to_string(material.dir.join("ca.key")).unwrap())
            .unwrap();
    let authority = rcgen::Issuer::from_ca_cert_pem(
        &std::fs::read_to_string(material.dir.join("ca.pem")).unwrap(),
        authority_key,
    )
    .unwrap();
    let expiring_key = rcgen::KeyPair::generate().unwrap();
    let mut expiring_params = rcgen::CertificateParams::new(vec!["expiring.test".into()]).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    expiring_params.not_after = x509_parser::time::ASN1Time::from_timestamp(now + 60)
        .unwrap()
        .to_datetime();
    let expiring = expiring_params
        .signed_by(&expiring_key, &authority)
        .unwrap();
    std::fs::write(material.dir.join("expiring.pem"), expiring.pem()).unwrap();
    std::fs::write(
        material.dir.join("expiring.key"),
        expiring_key.serialize_pem(),
    )
    .unwrap();

    profile
        .options
        .certificates
        .push(zero_traits::TlsCertificateFiles {
            cert_path: "expiring.pem".into(),
            key_path: "expiring.key".into(),
            ocsp_path: None,
            ocsp_stapling_secs: 0,
            usage: zero_traits::TlsCertificateUsage::Encipherment,
            build_chain: false,
        });
    profile
        .options
        .certificates
        .push(zero_traits::TlsCertificateFiles {
            cert_path: "ca.pem".into(),
            key_path: "ca.key".into(),
            ocsp_path: None,
            ocsp_stapling_secs: 0,
            usage: zero_traits::TlsCertificateUsage::AuthorityIssue,
            build_chain: true,
        });
    let server = Arc::new(tls::build_server_config(&profile, Some(&material.dir), false).unwrap());
    let chain = exchange_chain(
        tls::config::client(&material.client(), Some(&material.dir), false).unwrap(),
        server,
        "expiring.test",
    )
    .await
    .unwrap();
    assert_eq!(chain.len(), 2);
    assert_ne!(chain[0].as_slice(), expiring.der().as_ref());
}

#[tokio::test]
async fn authority_refresh_uses_new_files_and_retains_the_last_good_configuration_on_error() {
    let material = Material::new();
    let mut profile = material.server();
    profile.options.reload_interval_secs = 1;
    profile
        .options
        .certificates
        .push(zero_traits::TlsCertificateFiles {
            cert_path: "ca.pem".into(),
            key_path: "ca.key".into(),
            ocsp_path: None,
            ocsp_stapling_secs: 0,
            usage: zero_traits::TlsCertificateUsage::AuthorityIssue,
            build_chain: true,
        });
    let server = Arc::new(tls::build_server_config(&profile, Some(&material.dir), false).unwrap());
    let mut client = material.client();
    client.insecure = true;
    let connect = || tls::config::client(&client, Some(&material.dir), false).unwrap();

    let original = exchange_chain(connect(), server.clone(), "before-refresh.test")
        .await
        .unwrap()[1]
        .clone();
    let replacement_key = rcgen::KeyPair::generate().unwrap();
    let mut replacement_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    replacement_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let replacement = replacement_params.self_signed(&replacement_key).unwrap();
    std::fs::write(material.dir.join("ca.pem"), replacement.pem()).unwrap();
    std::fs::write(material.dir.join("ca.key"), replacement_key.serialize_pem()).unwrap();

    let refreshed = tokio::time::timeout(Duration::from_secs(4), async {
        let mut attempt = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let chain = exchange_chain(
                connect(),
                server.clone(),
                &format!("after-refresh-{attempt}.test"),
            )
            .await
            .unwrap();
            if chain[1] != original {
                break chain[1].clone();
            }
            attempt += 1;
        }
    })
    .await
    .expect("TLS authority did not refresh");
    assert_eq!(refreshed.as_slice(), replacement.der().as_ref());

    std::fs::write(material.dir.join("ca.key"), "invalid key").unwrap();
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let retained = exchange_chain(connect(), server, "after-invalid-refresh.test")
        .await
        .unwrap();
    assert_eq!(retained[1], refreshed);
}

#[tokio::test]
async fn fingerprint_negotiates_tls12_and_tls13_with_opt_in_resumption() {
    for version in ["1.2", "1.3"] {
        for fingerprint in ["chrome-83", "chrome", "firefox"] {
            let material = Material::new();
            let mut server = material.server();
            server.options.parameters.min_version = version.into();
            server.options.parameters.max_version = version.into();
            server.options.parameters.enable_session_resumption = true;
            let server =
                Arc::new(tls::build_server_config(&server, Some(&material.dir), false).unwrap());
            let mut profile = material.client();
            profile.client_fingerprint = Some(fingerprint.into());
            profile.options.parameters.enable_session_resumption = true;
            for expected in [rustls::HandshakeKind::Full, rustls::HandshakeKind::Resumed] {
                let config = tls::config::client(&profile, Some(&material.dir), false).unwrap();
                assert!(config.client_hello_profile.is_some());
                let (negotiated, kind, _) =
                    exchange(config, server.clone(), "one.test").await.unwrap();
                assert_eq!(
                    negotiated,
                    if version == "1.2" {
                        rustls::ProtocolVersion::TLSv1_2
                    } else {
                        rustls::ProtocolVersion::TLSv1_3
                    }
                );
                assert_eq!(kind, expected);
            }
        }
    }
}
