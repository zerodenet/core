//! Prepared multi-certificate SNI selection and server TLS policy.
use super::{
    certificates::{load_certs, load_private_key, resolve_path},
    config::{invalid, provider, versions},
};
use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use std::{
    collections::{HashMap, VecDeque},
    io,
    path::Path,
    sync::{Arc, Mutex},
};
use zero_traits::{ServerTlsProfile, TlsCertificateUsage};

#[derive(Clone, Debug)]
pub(super) struct Certificate {
    names: Vec<String>,
    pub(super) key: Arc<CertifiedKey>,
    not_after: i64,
}

const ISSUED_CERTIFICATE_CACHE_CAPACITY: usize = 1024;

#[derive(Debug, Default)]
struct IssuedCertificateCache {
    values: HashMap<String, super::authority::IssuedCertificate>,
    recent: VecDeque<String>,
}

impl IssuedCertificateCache {
    fn get(&mut self, name: &str, current_time: i64) -> Option<Arc<CertifiedKey>> {
        self.values
            .retain(|_, value| !super::authority::expires_soon(value.not_after, current_time));
        self.recent.retain(|name| self.values.contains_key(name));
        let key = self.values.get(name)?.key.clone();
        self.recent.retain(|entry| entry != name);
        self.recent.push_back(name.to_owned());
        Some(key)
    }

    fn insert(&mut self, name: String, certificate: super::authority::IssuedCertificate) {
        self.recent.retain(|entry| entry != &name);
        while self.values.len() >= ISSUED_CERTIFICATE_CACHE_CAPACITY {
            let Some(oldest) = self.recent.pop_front() else {
                self.values.clear();
                break;
            };
            self.values.remove(&oldest);
        }
        self.recent.push_back(name.clone());
        self.values.insert(name, certificate);
    }
}

#[derive(Debug)]
struct Resolver {
    certificates: super::refresh::Certificates,
    authorities: super::refresh::Authorities,
    issued: Mutex<IssuedCertificateCache>,
    provider: Arc<rustls::crypto::CryptoProvider>,
    reject_unknown_sni: bool,
}
fn matches_name(pattern: &str, name: &str) -> bool {
    pattern == name
        || pattern
            .strip_prefix("*.")
            .is_some_and(|suffix| name.split_once('.').is_some_and(|(_, rest)| rest == suffix))
}
impl ResolvesServerCert for Resolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let name = hello.server_name().unwrap_or("").to_ascii_lowercase();
        let certificates = self.certificates.current();
        let authorities = self.authorities.current();
        let current_time = super::authority::now();
        let found = certificates.iter().find(|cert| {
            cert.names
                .iter()
                .any(|pattern| matches_name(pattern, &name))
        });
        if let Some(found) = found.filter(|certificate| {
            authorities.is_empty()
                || !super::authority::expires_soon(certificate.not_after, current_time)
        }) {
            return Some(found.key.clone());
        }

        if !name.is_empty() && !authorities.is_empty() {
            let mut issued = self.issued.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(certificate) = issued.get(&name, current_time) {
                return Some(certificate);
            }
            for authority in authorities.iter() {
                match authority.issue(&name, &self.provider) {
                    Ok(certificate) => {
                        tracing::info!(
                            sni = %name,
                            not_after = certificate.not_after,
                            "TLS certificate issued from configured authority"
                        );
                        let key = certificate.key.clone();
                        issued.insert(name.clone(), certificate);
                        return Some(key);
                    }
                    Err(error) => {
                        tracing::warn!(%error, sni = %name, "TLS authority could not issue certificate")
                    }
                }
            }
            return None;
        }

        (!self.reject_unknown_sni)
            .then(|| certificates.first())
            .flatten()
            .map(|cert| cert.key.clone())
    }
}
pub fn server(
    profile: &(impl ServerTlsProfile + ?Sized),
    base_dir: Option<&Path>,
    quic: bool,
) -> io::Result<rustls::ServerConfig> {
    let options = profile.tls_options();
    ztls::settings::validate_server(&options, quic).map_err(invalid)?;
    let provider = provider(&options.parameters, profile.server_fingerprint(), false)?;
    let primary = match (
        profile.cert_path().is_empty(),
        profile.key_path().is_empty(),
    ) {
        (false, false) => Some(zero_traits::TlsCertificateFiles {
            cert_path: profile.cert_path().into(),
            key_path: profile.key_path().into(),
            ocsp_path: None,
            ocsp_stapling_secs: options.ocsp_stapling_secs,
            usage: TlsCertificateUsage::Encipherment,
            build_chain: false,
        }),
        (true, true) => None,
        _ => {
            return Err(invalid(
                "TLS certificate and key paths must be configured together",
            ))
        }
    };
    let files: Vec<_> = primary
        .into_iter()
        .chain(options.certificates.iter().cloned())
        .map(|mut files| {
            files.cert_path = resolve_path(base_dir, &files.cert_path)
                .to_string_lossy()
                .into_owned();
            files.key_path = resolve_path(base_dir, &files.key_path)
                .to_string_lossy()
                .into_owned();
            files.ocsp_path = files
                .ocsp_path
                .as_deref()
                .map(|path| resolve_path(base_dir, path).to_string_lossy().into_owned());
            files
        })
        .collect();
    let (authority_files, certificate_files): (Vec<_>, Vec<_>) = files
        .into_iter()
        .partition(|files| files.usage == TlsCertificateUsage::AuthorityIssue);
    if authority_files.is_empty() && certificate_files.is_empty() {
        return Err(invalid(
            "TLS server has no certificate or issuing authority",
        ));
    }
    let certificates = certificate_files
        .iter()
        .map(|files| load(files, &provider))
        .collect::<Result<Vec<_>, _>>()?;
    let certificates = super::refresh::Certificates::new(
        certificates,
        certificate_files,
        provider.clone(),
        &options,
    );
    let authorities = authority_files
        .iter()
        .map(|files| super::authority::Authority::load(files, &provider).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    let authorities =
        super::refresh::Authorities::new(authorities, authority_files, provider.clone(), &options);
    let mut config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions(&options.parameters, quic)?)
        .map_err(invalid)?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(Resolver {
            certificates,
            authorities,
            issued: Mutex::new(IssuedCertificateCache::default()),
            provider: provider.clone(),
            reject_unknown_sni: options.reject_unknown_sni,
        }));
    config.alpn_protocols = profile
        .alpn()
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect();
    if options.parameters.enable_session_resumption {
        config.ticketer = rustls::crypto::ring::Ticketer::new().map_err(invalid)?;
    } else {
        config.send_tls13_tickets = 0;
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    }
    Ok(config)
}
pub(super) fn load(
    files: &zero_traits::TlsCertificateFiles,
    provider: &rustls::crypto::CryptoProvider,
) -> io::Result<Certificate> {
    let base_dir = None;

    let certs = load_certs(&resolve_path(base_dir, &files.cert_path)).map_err(invalid)?;
    let key = load_private_key(&resolve_path(base_dir, &files.key_path)).map_err(invalid)?;
    let mut key = CertifiedKey::from_der(certs, key, &provider).map_err(invalid)?;
    let (_, parsed) =
        x509_parser::parse_x509_certificate(key.end_entity_cert().map_err(invalid)?.as_ref())
            .map_err(invalid)?;
    let mut names: Vec<_> = parsed
        .subject()
        .iter_common_name()
        .filter_map(|name| name.as_str().ok())
        .map(str::to_ascii_lowercase)
        .collect();
    if let Some(san) = parsed.subject_alternative_name().map_err(invalid)? {
        names.extend(
            san.value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    x509_parser::extensions::GeneralName::DNSName(name) => {
                        Some(name.to_ascii_lowercase())
                    }
                    _ => None,
                }),
        );
    }
    let not_after = parsed.validity().not_after.timestamp();
    if let Some(path) = &files.ocsp_path {
        key.ocsp = Some(std::fs::read(resolve_path(base_dir, path))?);
    }
    Ok(Certificate {
        names,
        key: Arc::new(key),
        not_after,
    })
}

#[cfg(test)]
#[path = "../../tests/tls/server.rs"]
mod tests;
