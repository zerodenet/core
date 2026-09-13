use super::config::invalid;
use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
    RootCertStore,
};
use std::{io, sync::Arc};
#[derive(Debug)]
pub(super) struct Verifier {
    signatures: crate::certificate_verifier::InsecureServerVerifier,
    roots: Arc<RootCertStore>,
    pins: Vec<[u8; 32]>,
    names: Vec<ServerName<'static>>,
    insecure: bool,
}
impl Verifier {
    pub fn new(
        provider: Arc<rustls::crypto::CryptoProvider>,
        roots: RootCertStore,
        insecure: bool,
        options: &zero_traits::ClientTlsOptions,
    ) -> io::Result<Self> {
        Ok(Self {
            signatures: crate::certificate_verifier::InsecureServerVerifier { provider },
            roots: Arc::new(roots),
            insecure,
            pins: options
                .pinned_peer_cert_sha256
                .iter()
                .map(|p| ztls::settings::certificate_pin(p).map_err(invalid))
                .collect::<Result<_, _>>()?,
            names: options
                .verify_peer_names
                .iter()
                .map(|name| ServerName::try_from(name.clone()).map_err(invalid))
                .collect::<Result<_, _>>()?,
        })
    }
    fn pinned(&self, cert: &CertificateDer<'_>) -> bool {
        let hash = ring::digest::digest(&ring::digest::SHA256, cert.as_ref());
        self.pins.iter().any(|pin| pin.as_slice() == hash.as_ref())
    }
}
impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let mut roots = self.roots.clone();
        if !self.pins.is_empty() {
            if self.pinned(leaf) {
                return Ok(ServerCertVerified::assertion());
            }
            let ca = intermediates
                .iter()
                .find(|cert| {
                    self.pinned(cert)
                        && x509_parser::parse_x509_certificate(cert.as_ref())
                            .is_ok_and(|(_, cert)| cert.is_ca())
                })
                .ok_or_else(|| {
                    rustls::Error::General(
                        "peer certificate does not match a configured pin".into(),
                    )
                })?;
            let mut pinned_roots = RootCertStore::empty();
            pinned_roots.add(ca.clone())?;
            roots = Arc::new(pinned_roots);
        } else if self.names.is_empty() && self.insecure {
            return Ok(ServerCertVerified::assertion());
        }
        let verifier =
            WebPkiServerVerifier::builder_with_provider(roots, self.signatures.provider.clone())
                .build()
                .map_err(|error| rustls::Error::General(error.to_string()))?;
        if self.names.is_empty() {
            return verifier.verify_server_cert(leaf, intermediates, name, ocsp, now);
        }
        let mut last = None;
        for name in &self.names {
            match verifier.verify_server_cert(leaf, intermediates, name, ocsp, now) {
                Ok(verified) => return Ok(verified),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signed: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.signatures
            .verify_tls12_signature(message, cert, signed)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signed: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.signatures
            .verify_tls13_signature(message, cert, signed)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.signatures.supported_verify_schemes()
    }
}
