use std::{
    io,
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};

use openssl::ocsp::OcspResponse;
use openssl::ssl::SslContextBuilder;
use rustls::pki_types::CertificateDer;

#[derive(Clone)]
pub(super) struct OcspTarget {
    certificates: Arc<Vec<CertificateDer<'static>>>,
    staple: Arc<RwLock<Option<Vec<u8>>>>,
    pub(super) interval: Duration,
}

impl OcspTarget {
    pub(super) async fn refresh(&self, client: &crate::http_client::HttpClient) {
        match super::super::ocsp::retrieve(client, &self.certificates).await {
            Ok(staple) => {
                if let Err(error) = OcspResponse::from_der(&staple) {
                    tracing::warn!(%error, "OpenSSL TLS OCSP refresh retained the previous staple");
                    return;
                }
                *self
                    .staple
                    .write()
                    .unwrap_or_else(|error| error.into_inner()) = Some(staple);
            }
            Err(error) => {
                tracing::warn!(%error, "OpenSSL TLS OCSP refresh retained the previous staple")
            }
        }
    }

    fn same_leaf(&self, other: &Self) -> bool {
        self.certificates.first() == other.certificates.first()
    }

    fn inherit(&self, previous: &Self) {
        if !self.same_leaf(previous) {
            return;
        }
        let previous = previous
            .staple
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        *self
            .staple
            .write()
            .unwrap_or_else(|error| error.into_inner()) = previous;
    }

    #[cfg(test)]
    pub(super) fn current(&self) -> Option<Vec<u8>> {
        self.staple
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

pub(super) fn apply(
    builder: &mut SslContextBuilder,
    files: &zero_traits::TlsCertificateFiles,
    targets: &mut Vec<OcspTarget>,
) -> io::Result<()> {
    let automatic = files.ocsp_stapling_secs != 0 && files.ocsp_path.is_none();
    if !automatic && files.ocsp_path.is_none() {
        return Ok(());
    }
    let initial = files.ocsp_path.as_deref().map(std::fs::read).transpose()?;
    if let Some(staple) = &initial {
        OcspResponse::from_der(staple).map_err(io::Error::other)?;
    }
    let staple = Arc::new(RwLock::new(initial));
    let callback_staple = staple.clone();
    builder
        .set_status_callback(move |ssl| {
            let staple = callback_staple
                .read()
                .unwrap_or_else(|error| error.into_inner());
            let Some(staple) = staple.as_deref() else {
                return Ok(false);
            };
            ssl.set_ocsp_status(staple)?;
            Ok(true)
        })
        .map_err(io::Error::other)?;
    if automatic {
        let certificates = super::super::certificates::load_certs(Path::new(&files.cert_path))?;
        targets.push(OcspTarget {
            certificates: Arc::new(certificates),
            staple,
            interval: Duration::from_secs(files.ocsp_stapling_secs.into()),
        });
    }
    Ok(())
}

pub(super) fn inherit(current: &[OcspTarget], previous: &[OcspTarget]) {
    for target in current {
        if let Some(previous) = previous
            .iter()
            .find(|candidate| target.same_leaf(candidate))
        {
            target.inherit(previous);
        }
    }
}
