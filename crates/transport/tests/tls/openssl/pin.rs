use std::{io, time::Duration};

use base64::Engine;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair,
};
use zero_traits::{ClientTlsOptions, ServerTlsOptions, TlsBackend};

use crate::profile::{OwnedClientTlsProfile, OwnedServerTlsProfile};

#[derive(Clone, Copy)]
enum LeafKind {
    Server,
    Expired,
    ClientOnly,
}

struct PinnedChain {
    _directory: tempfile::TempDir,
    cert_path: String,
    key_path: String,
    ca_cert_path: Option<String>,
    authority_pin: String,
}

impl PinnedChain {
    fn new(kind: LeafKind, authority_is_ca: bool) -> Self {
        Self::build(kind, authority_is_ca, false)
    }

    fn with_unrelated_pinned_authority() -> Self {
        Self::build(LeafKind::Server, true, true)
    }

    fn build(kind: LeafKind, authority_is_ca: bool, unrelated_authority: bool) -> Self {
        let root_key = KeyPair::generate().unwrap();
        let mut root = CertificateParams::new(Vec::<String>::new()).unwrap();
        root.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        root.distinguished_name = distinguished_name("pin test root");
        let root_cert = root.self_signed(&root_key).unwrap();

        let authority_key = KeyPair::generate().unwrap();
        let mut authority = CertificateParams::new(Vec::<String>::new()).unwrap();
        if authority_is_ca {
            authority.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        }
        authority.distinguished_name = distinguished_name("pinned authority");
        let authority_cert = authority
            .signed_by(&authority_key, &Issuer::from_params(&root, &root_key))
            .unwrap();

        let leaf_key = KeyPair::generate().unwrap();
        let mut leaf = CertificateParams::new(vec!["pinned.example".into()]).unwrap();
        leaf.extended_key_usages = vec![match kind {
            LeafKind::Server | LeafKind::Expired => ExtendedKeyUsagePurpose::ServerAuth,
            LeafKind::ClientOnly => ExtendedKeyUsagePurpose::ClientAuth,
        }];
        if matches!(kind, LeafKind::Expired) {
            leaf.not_before = rcgen::date_time_ymd(2018, 1, 1);
            leaf.not_after = rcgen::date_time_ymd(2019, 1, 1);
        }
        let (leaf, signing_authority) = if unrelated_authority {
            let signer_key = KeyPair::generate().unwrap();
            let mut signer = CertificateParams::new(Vec::<String>::new()).unwrap();
            signer.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            signer.distinguished_name = distinguished_name("signing authority");
            let signer_cert = signer
                .signed_by(&signer_key, &Issuer::from_params(&root, &root_key))
                .unwrap();
            (
                leaf.signed_by(&leaf_key, &Issuer::from_params(&signer, &signer_key))
                    .unwrap(),
                Some(signer_cert),
            )
        } else {
            (
                leaf.signed_by(&leaf_key, &Issuer::from_params(&authority, &authority_key))
                    .unwrap(),
                None,
            )
        };

        let directory = tempfile::tempdir().unwrap();
        let cert_path = directory.path().join("server.pem");
        let key_path = directory.path().join("server.key");
        let ca_cert_path = unrelated_authority.then(|| directory.path().join("root.pem"));
        let mut server_chain = leaf.pem();
        if let Some(signer) = signing_authority {
            server_chain.push_str(&signer.pem());
        }
        server_chain.push_str(&authority_cert.pem());
        std::fs::write(&cert_path, server_chain).unwrap();
        std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
        if let Some(path) = &ca_cert_path {
            std::fs::write(path, root_cert.pem()).unwrap();
        }
        let authority_pin = base64::engine::general_purpose::STANDARD
            .encode(openssl::sha::sha256(authority_cert.der().as_ref()));
        Self {
            _directory: directory,
            cert_path: cert_path.to_string_lossy().into_owned(),
            key_path: key_path.to_string_lossy().into_owned(),
            ca_cert_path: ca_cert_path.map(|path| path.to_string_lossy().into_owned()),
            authority_pin,
        }
    }

    fn server(&self) -> OwnedServerTlsProfile {
        OwnedServerTlsProfile {
            options: ServerTlsOptions {
                backend: TlsBackend::OpenSsl,
                one_time_loading: true,
                ..Default::default()
            },
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            alpn: Vec::new(),
            server_fingerprint: None,
        }
    }

    fn client(&self, insecure: bool) -> OwnedClientTlsProfile {
        OwnedClientTlsProfile {
            options: ClientTlsOptions {
                backend: TlsBackend::OpenSsl,
                disable_system_roots: true,
                pinned_peer_cert_sha256: vec![self.authority_pin.clone()],
                ..Default::default()
            },
            server_name: Some("pinned.example".into()),
            disable_sni: false,
            ca_cert_path: self.ca_cert_path.clone(),
            insecure,
            alpn: Vec::new(),
            client_fingerprint: None,
        }
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}

async fn connects(material: &PinnedChain, insecure: bool) -> bool {
    connects_with_client(material, material.client(insecure)).await
}

async fn connects_with_client(material: &PinnedChain, client: OwnedClientTlsProfile) -> bool {
    let server =
        super::super::super::OpenSslServerContext::build(&material.server(), None).unwrap();
    timeout_handshake(async {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (client, _server) = tokio::join!(
            super::super::super::client::connect(client_io, &client, None, "pinned.example"),
            super::super::super::stream::OpenSslTlsStream::accept(&server, server_io),
        );
        Ok(client.is_ok())
    })
    .await
    .unwrap_or(false)
}

async fn timeout_handshake(
    handshake: impl std::future::Future<Output = io::Result<bool>>,
) -> io::Result<bool> {
    tokio::time::timeout(Duration::from_secs(3), handshake)
        .await
        .map_err(io::Error::other)?
}

#[tokio::test]
async fn pinned_ca_is_a_real_trust_anchor_for_a_valid_server_chain() {
    let material = PinnedChain::new(LeafKind::Server, true);
    assert!(connects(&material, false).await);
}

#[tokio::test]
async fn pinned_chain_entry_does_not_bypass_ca_time_or_server_usage_checks() {
    for (kind, authority_is_ca) in [
        (LeafKind::Server, false),
        (LeafKind::Expired, true),
        (LeafKind::ClientOnly, true),
    ] {
        let material = PinnedChain::new(kind, authority_is_ca);
        assert!(!connects(&material, false).await);
    }
}

#[tokio::test]
async fn insecure_does_not_bypass_pinned_ca_time_or_server_usage_checks() {
    for kind in [LeafKind::Expired, LeafKind::ClientOnly] {
        let material = PinnedChain::new(kind, true);
        assert!(!connects(&material, true).await);
    }
}

#[tokio::test]
async fn unrelated_pinned_ca_does_not_ride_an_ordinary_valid_path() {
    let material = PinnedChain::with_unrelated_pinned_authority();
    assert!(!connects(&material, false).await);
}

#[tokio::test]
async fn insecure_with_an_explicit_name_still_requires_a_valid_path_and_that_name() {
    let material = PinnedChain::with_unrelated_pinned_authority();
    let mut client = material.client(true);
    client.options.pinned_peer_cert_sha256.clear();
    client.options.verify_peer_names = vec!["pinned.example".into()];
    assert!(connects_with_client(&material, client.clone()).await);

    client.options.verify_peer_names = vec!["other.example".into()];
    assert!(!connects_with_client(&material, client).await);

    let untrusted = PinnedChain::new(LeafKind::Server, true);
    let mut client = untrusted.client(true);
    client.options.pinned_peer_cert_sha256.clear();
    client.options.verify_peer_names = vec!["pinned.example".into()];
    assert!(!connects_with_client(&untrusted, client).await);
}
