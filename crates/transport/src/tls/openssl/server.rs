use std::{
    collections::{HashMap, VecDeque},
    io,
    path::Path,
    sync::{Arc, Mutex},
};

use foreign_types::ForeignTypeRef;
use openssl::{
    pkey::PKey,
    ssl::{
        AlpnError, NameType, SniError, SslContext, SslContextBuilder, SslContextRef, SslFiletype,
        SslMethod, SslSessionCacheMode,
    },
    x509::X509,
};
use zero_traits::{ServerTlsProfile, TlsCertificateFiles, TlsCertificateUsage};

use crate::profile::OwnedServerTlsProfile;

use super::{
    certificates::{files as certificate_files, matches_name, names as certificate_names},
    context::PreparedContext,
};

const ISSUED_CONTEXT_CACHE_CAPACITY: usize = 1024;

#[derive(Default)]
struct IssuedContextCache {
    values: HashMap<String, IssuedContext>,
    recent: VecDeque<String>,
}

struct IssuedContext {
    context: SslContext,
    not_after: i64,
}

impl IssuedContextCache {
    fn get(&mut self, name: &str, current_time: i64) -> Option<SslContext> {
        self.values
            .retain(|_, value| !crate::tls::authority::expires_soon(value.not_after, current_time));
        self.recent.retain(|name| self.values.contains_key(name));
        let context = self.values.get(name)?.context.clone();
        self.recent.retain(|entry| entry != name);
        self.recent.push_back(name.to_owned());
        Some(context)
    }

    fn insert(&mut self, name: String, value: IssuedContext) {
        self.recent.retain(|entry| entry != &name);
        while self.values.len() >= ISSUED_CONTEXT_CACHE_CAPACITY {
            let Some(oldest) = self.recent.pop_front() else {
                self.values.clear();
                break;
            };
            self.values.remove(&oldest);
        }
        self.recent.push_back(name.clone());
        self.values.insert(name, value);
    }
}

pub(super) fn build_prepared(
    profile: &OwnedServerTlsProfile,
    base_dir: Option<&Path>,
) -> io::Result<PreparedContext> {
    let options = profile.tls_options();
    ztls::settings::validate_server(&options, false).map_err(io::Error::other)?;
    if profile.server_fingerprint().is_some() {
        return Err(invalid(
            "OpenSSL TLS does not apply rustls server fingerprint presets",
        ));
    }
    let files = certificate_files(profile, base_dir)?;
    let (authority_files, certificate_files): (Vec<_>, Vec<_>) = files
        .into_iter()
        .partition(|file| file.usage == TlsCertificateUsage::AuthorityIssue);
    if authority_files.is_empty() && certificate_files.is_empty() {
        return Err(invalid(
            "OpenSSL TLS server has no certificate or issuing authority",
        ));
    }
    let authorities: Arc<Vec<Arc<crate::tls::authority::Authority>>> = Arc::new(
        authority_files
            .iter()
            .map(|file| crate::tls::authority::Authority::load_for_issuance(file).map(Arc::new))
            .collect::<io::Result<Vec<_>>>()?,
    );
    let mut ocsp_targets = Vec::new();
    let mut primary_names = Vec::new();
    let mut alternatives = Vec::with_capacity(certificate_files.len().saturating_sub(1));
    for (index, file) in certificate_files.iter().enumerate() {
        let certificate =
            X509::from_pem(&std::fs::read(&file.cert_path)?).map_err(io::Error::other)?;
        let names = certificate_names(&certificate);
        if index == 0 {
            primary_names = names;
        } else {
            alternatives.push((names, build_context(profile, file, &mut ocsp_targets)?));
        }
    }
    let alternatives: Arc<Vec<(Vec<String>, SslContext)>> = Arc::new(alternatives);
    let mut primary = build_context_builder(profile, certificate_files.first(), &mut ocsp_targets)?;
    let reject_unknown = options.reject_unknown_sni;
    let issued = Arc::new(Mutex::new(IssuedContextCache::default()));
    let issued_profile = profile.clone();
    primary.set_servername_callback(move |ssl, _alert| {
        let name = ssl
            .servername(NameType::HOST_NAME)
            .unwrap_or("")
            .to_ascii_lowercase();
        if primary_names
            .iter()
            .any(|pattern| matches_name(pattern, &name))
        {
            return Ok(());
        }
        if let Some((_, context)) = alternatives
            .iter()
            .find(|(names, _)| names.iter().any(|pattern| matches_name(pattern, &name)))
        {
            ssl.set_ssl_context(context)
                .map_err(|_| SniError::ALERT_FATAL)?;
            return Ok(());
        }
        if !name.is_empty() && !authorities.is_empty() {
            let current_time = crate::tls::authority::now();
            if let Some(context) = issued
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&name, current_time)
            {
                ssl.set_ssl_context(&context)
                    .map_err(|_| SniError::ALERT_FATAL)?;
                return Ok(());
            }
            for authority in authorities.iter() {
                let Ok(material) = authority.issue_material(&name) else {
                    continue;
                };
                let Ok(context) = build_issued_context(&issued_profile, &material) else {
                    continue;
                };
                ssl.set_ssl_context(&context)
                    .map_err(|_| SniError::ALERT_FATAL)?;
                issued
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(
                        name.clone(),
                        IssuedContext {
                            context,
                            not_after: material.not_after,
                        },
                    );
                return Ok(());
            }
            return Err(SniError::ALERT_FATAL);
        }
        if certificate_files.is_empty() {
            return Err(SniError::ALERT_FATAL);
        }
        if reject_unknown {
            Err(SniError::ALERT_FATAL)
        } else {
            Ok(())
        }
    });
    super::ech::apply(&mut primary, &options.ech_server_keys)?;
    Ok(PreparedContext {
        context: primary.build(),
        ocsp_targets,
    })
}

fn build_context(
    profile: &(impl ServerTlsProfile + ?Sized),
    files: &TlsCertificateFiles,
    ocsp_targets: &mut Vec<super::ocsp::OcspTarget>,
) -> io::Result<SslContext> {
    Ok(build_context_builder(profile, Some(files), ocsp_targets)?.build())
}

fn build_context_builder(
    profile: &(impl ServerTlsProfile + ?Sized),
    files: Option<&TlsCertificateFiles>,
    ocsp_targets: &mut Vec<super::ocsp::OcspTarget>,
) -> io::Result<SslContextBuilder> {
    let options = profile.tls_options();
    let mut builder = SslContextBuilder::new(SslMethod::tls_server()).map_err(io::Error::other)?;
    super::parameters::apply(&mut builder, &options.parameters)?;
    builder.set_read_ahead(false);
    if let Some(files) = files {
        builder
            .set_certificate_chain_file(&files.cert_path)
            .map_err(io::Error::other)?;
        builder
            .set_private_key_file(&files.key_path, SslFiletype::PEM)
            .map_err(io::Error::other)?;
        builder.check_private_key().map_err(io::Error::other)?;
        super::ocsp::apply(&mut builder, files, ocsp_targets)?;
    }
    let protocols = encode_alpn(profile.alpn())?;
    if !protocols.is_empty() {
        builder.set_alpn_select_callback(move |_ssl, client| {
            select_alpn(&protocols, client).ok_or(AlpnError::NOACK)
        });
    }
    if options.parameters.enable_session_resumption {
        builder.set_session_cache_mode(SslSessionCacheMode::SERVER);
        // The builder owns this live SSL_CTX; borrow it only to copy the leaf DER.
        let context = unsafe { SslContextRef::from_ptr(builder.as_ptr()) };
        if let Some(certificate) = context.certificate() {
            let certificate = certificate.to_der().map_err(io::Error::other)?;
            builder
                .set_session_id_context(&openssl::sha::sha256(&certificate))
                .map_err(io::Error::other)?;
        }
    } else {
        builder.set_session_cache_mode(SslSessionCacheMode::OFF);
        builder.set_num_tickets(0).map_err(io::Error::other)?;
    }
    Ok(builder)
}

fn build_issued_context(
    profile: &(impl ServerTlsProfile + ?Sized),
    material: &crate::tls::authority::IssuedCertificateMaterial,
) -> io::Result<SslContext> {
    let mut certificates = material.certificates.iter();
    let leaf = certificates
        .next()
        .ok_or_else(|| invalid("TLS issuing authority produced an empty certificate chain"))?;
    let mut builder = build_context_builder(profile, None, &mut Vec::new())?;
    let leaf = X509::from_der(leaf).map_err(io::Error::other)?;
    builder.set_certificate(&leaf).map_err(io::Error::other)?;
    for certificate in certificates {
        builder
            .add_extra_chain_cert(X509::from_der(certificate).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }
    let private_key =
        PKey::private_key_from_der(&material.private_key).map_err(io::Error::other)?;
    builder
        .set_private_key(&private_key)
        .map_err(io::Error::other)?;
    builder.check_private_key().map_err(io::Error::other)?;
    if profile.tls_options().parameters.enable_session_resumption {
        builder
            .set_session_id_context(&openssl::sha::sha256(
                material
                    .certificates
                    .first()
                    .expect("issued certificate chain was checked above"),
            ))
            .map_err(io::Error::other)?;
    }
    Ok(builder.build())
}

fn encode_alpn(protocols: &[String]) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for protocol in protocols {
        let length: u8 = protocol
            .len()
            .try_into()
            .map_err(|_| invalid("TLS ALPN protocol exceeds 255 bytes"))?;
        if length == 0 {
            return Err(invalid("TLS ALPN protocol must not be empty"));
        }
        encoded.push(length);
        encoded.extend_from_slice(protocol.as_bytes());
    }
    Ok(encoded)
}

fn select_alpn<'a>(server: &[u8], client: &'a [u8]) -> Option<&'a [u8]> {
    let mut server_position = 0usize;
    while server_position < server.len() {
        let server_length = usize::from(*server.get(server_position)?);
        let server_protocol =
            server.get(server_position + 1..server_position + 1 + server_length)?;
        let mut client_position = 0usize;
        while client_position < client.len() {
            let client_length = usize::from(*client.get(client_position)?);
            let client_protocol =
                client.get(client_position + 1..client_position + 1 + client_length)?;
            if client_protocol == server_protocol {
                return Some(client_protocol);
            }
            client_position += client_length + 1;
        }
        server_position += server_length + 1;
    }
    None
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
