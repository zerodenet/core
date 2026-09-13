use std::{io, path::Path};

use openssl::{nid::Nid, x509::X509};
use zero_traits::{ServerTlsProfile, TlsCertificateFiles, TlsCertificateUsage};

pub(super) fn files(
    profile: &(impl ServerTlsProfile + ?Sized),
    base_dir: Option<&Path>,
) -> io::Result<Vec<TlsCertificateFiles>> {
    let options = profile.tls_options();
    let primary = match (
        profile.cert_path().is_empty(),
        profile.key_path().is_empty(),
    ) {
        (false, false) => Some(TlsCertificateFiles {
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
    primary
        .into_iter()
        .chain(options.certificates)
        .map(|mut files| {
            files.cert_path = super::super::certificates::resolve_path(base_dir, &files.cert_path)
                .to_string_lossy()
                .into_owned();
            files.key_path = super::super::certificates::resolve_path(base_dir, &files.key_path)
                .to_string_lossy()
                .into_owned();
            files.ocsp_path = files.ocsp_path.as_deref().map(|path| {
                super::super::certificates::resolve_path(base_dir, path)
                    .to_string_lossy()
                    .into_owned()
            });
            Ok(files)
        })
        .collect()
}

pub(super) fn names(certificate: &X509) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(alternative_names) = certificate.subject_alt_names() {
        names.extend(
            alternative_names
                .iter()
                .filter_map(|name| name.dnsname())
                .map(str::to_ascii_lowercase),
        );
    }
    names.extend(
        certificate
            .subject_name()
            .entries_by_nid(Nid::COMMONNAME)
            .filter_map(|entry| entry.data().to_string().ok())
            .map(|name| name.to_ascii_lowercase()),
    );
    names
}

pub(super) fn matches_name(pattern: &str, name: &str) -> bool {
    pattern == name
        || pattern
            .strip_prefix("*.")
            .is_some_and(|suffix| name.split_once('.').is_some_and(|(_, rest)| rest == suffix))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
