//! Shared TLS policy materialization for TCP, relays and QUIC.
use super::certificates::{load_certs, resolve_path};
use rustls::{ClientConfig, RootCertStore};
use std::{io, path::Path, sync::Arc};
use zero_traits::{ClientTlsProfile, TlsParameters};

pub(super) fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

pub(super) fn provider(
    options: &TlsParameters,
    fingerprint: Option<&str>,
    client: bool,
) -> io::Result<Arc<rustls::crypto::CryptoProvider>> {
    let mut provider = fingerprint
        .and_then(crate::fingerprint::lookup_fingerprint)
        .map(|preset| {
            if client {
                crate::fingerprint::build_client_provider(&preset)
            } else {
                crate::fingerprint::build_provider(&preset)
            }
        })
        .unwrap_or_else(rustls::crypto::ring::default_provider);
    if client {
        if let Some(preset) = fingerprint.and_then(crate::fingerprint::lookup_fingerprint) {
            super::fingerprint::configure_provider(&mut provider, preset.client_hello_profile)?;
        }
    }
    if !options.cipher_suites.is_empty() {
        // Go's CipherSuites selects TLS 1.2 suites; TLS 1.3 suites remain automatic.
        let available = rustls::crypto::ring::default_provider().cipher_suites;
        let mut suites: Vec<_> = provider
            .cipher_suites
            .iter()
            .filter(|suite| suite.version().version == rustls::ProtocolVersion::TLSv1_3)
            .copied()
            .collect();
        for name in &options.cipher_suites {
            let id = ztls::settings::cipher_suite(name).map_err(invalid)?;
            if id >= 0x1301 && id <= 0x1303 {
                continue;
            }
            let suite = available
                .iter()
                .find(|suite| u16::from(suite.suite()) == id)
                .ok_or_else(|| invalid("TLS cipher suite unavailable"))?;
            if !suites.contains(suite) {
                suites.push(*suite);
            }
        }
        provider.cipher_suites = suites;
    }
    if !options.curve_preferences.is_empty() {
        let mut groups = Vec::new();
        for name in &options.curve_preferences {
            let id = ztls::settings::curve(name).map_err(invalid)?;
            let group = super::groups::lookup(id)
                .ok_or_else(|| invalid("TLS key exchange group unavailable"))?;
            if !groups
                .iter()
                .any(|g: &&dyn rustls::crypto::SupportedKxGroup| g.name() == group.name())
            {
                groups.push(group);
            }
        }
        provider.kx_groups = groups;
    }
    Ok(Arc::new(provider))
}
pub(super) fn versions(
    options: &TlsParameters,
    quic: bool,
) -> io::Result<Vec<&'static rustls::SupportedProtocolVersion>> {
    ztls::settings::validate_parameters(options, quic).map_err(invalid)?;
    let min = ztls::settings::version(&options.min_version, if quic { 0x304 } else { 0x303 })
        .map_err(invalid)?;
    let max = ztls::settings::version(&options.max_version, 0x304).map_err(invalid)?;
    Ok([&rustls::version::TLS13, &rustls::version::TLS12]
        .into_iter()
        .filter(|v| {
            let id = u16::from(v.version);
            id >= min && id <= max && (!quic || id == 0x304)
        })
        .collect())
}

pub(super) fn roots(
    profile: &(impl ClientTlsProfile + ?Sized),
    base_dir: Option<&Path>,
) -> io::Result<(RootCertStore, Vec<u8>)> {
    let options = profile.tls_options();
    let mut roots = if options.disable_system_roots {
        RootCertStore::empty()
    } else {
        RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
    };
    let mut ca_hash = ring::digest::Context::new(&ring::digest::SHA256);
    if let Some(path) = profile.ca_cert_path() {
        for cert in load_certs(&resolve_path(base_dir, path)).map_err(invalid)? {
            ca_hash.update(&(cert.len() as u64).to_be_bytes());
            ca_hash.update(cert.as_ref());
            roots.add(cert).map_err(invalid)?;
        }
    }
    Ok((roots, ca_hash.finish().as_ref().to_vec()))
}

pub fn client(
    profile: &(impl ClientTlsProfile + ?Sized),
    base_dir: Option<&Path>,
    quic: bool,
) -> io::Result<ClientConfig> {
    let options = profile.tls_options();
    ztls::settings::validate_client(&options, quic).map_err(invalid)?;
    let ech_configured = options.ech.source().map_err(invalid)?.is_some();
    let mut provider_parameters = options.parameters.clone();
    if ech_configured {
        // ECH always negotiates TLS 1.3. Go's CipherSuites setting only
        // selects pre-TLS-1.3 suites, so it cannot constrain this handshake.
        provider_parameters.cipher_suites.clear();
    }
    let provider = provider(&provider_parameters, profile.client_fingerprint(), true)?;
    let (roots, ca_hash) = roots(profile, base_dir)?;
    let cache_key = options.parameters.enable_session_resumption.then(|| {
        format!(
            "{:?}|{:?}|{}|{:?}",
            crate::profile::OwnedClientTlsProfile::from_profile(profile),
            base_dir,
            quic,
            ca_hash
        )
    });
    if let Some(config) = cache_key.as_deref().and_then(super::cache::get) {
        return Ok(config);
    }
    let verifier =
        super::verifier::Verifier::new(provider.clone(), roots, profile.insecure(), &options)?;
    let ech = super::ech::config_list(&options)?;
    let builder = ClientConfig::builder_with_provider(provider);
    let builder = match ech {
        Some(bytes) => {
            let hpke_suites = super::ech::supported_suites();
            let config = rustls::client::EchConfig::new(
                rustls::pki_types::EchConfigListBytes::from(bytes),
                &hpke_suites,
            )
            .map_err(invalid)?;
            builder
                .with_ech(rustls::client::EchMode::Enable(config))
                .map_err(invalid)?
        }
        None => builder
            .with_protocol_versions(&versions(&options.parameters, quic)?)
            .map_err(invalid)?,
    };
    let mut config = builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    config.enable_sni = !profile.disable_sni();
    config.alpn_protocols = profile
        .alpn()
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect();
    if !options.parameters.enable_session_resumption {
        config.resumption = rustls::client::Resumption::disabled();
    }
    if !quic {
        super::fingerprint::install(&mut config, profile)?;
    }
    Ok(match cache_key {
        Some(key) => super::cache::insert(key, config),
        None => config,
    })
}
