use super::*;
use rustls::{
    client::{danger::ServerCertVerifier, WebPkiServerVerifier},
    internal::msgs::codec::{Codec, Reader},
    pki_types::{CertificateDer, ServerName, UnixTime},
    DigitallySignedStruct, RootCertStore,
};
use std::sync::{Arc, OnceLock};

/// Trust policy for a real TLS server reached through a camouflage endpoint.
#[derive(Clone, Debug)]
pub struct ServerTrust(Arc<dyn ServerCertVerifier>);
impl ServerTrust {
    pub fn public_roots() -> io::Result<Self> {
        static VERIFIER: OnceLock<ServerTrust> = OnceLock::new();
        if let Some(verifier) = VERIFIER.get() {
            return Ok(verifier.clone());
        }
        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let verifier = Self::from_roots(roots)?;
        Ok(VERIFIER.get_or_init(|| verifier).clone())
    }
    fn from_roots(roots: RootCertStore) -> io::Result<Self> {
        WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map(|verifier| Self(verifier))
        .map_err(io::Error::other)
    }
    /// Explicit trust anchors for callers that own their TLS trust configuration.
    pub fn certificates(roots: &[Vec<u8>]) -> io::Result<Self> {
        let mut store = RootCertStore::empty();
        for der in roots {
            store
                .add(CertificateDer::from(der.clone()))
                .map_err(io::Error::other)?;
        }
        Self::from_roots(store)
    }
    pub fn verify(&self, message: &[u8], server_name: &str) -> io::Result<VerifiedServer> {
        let chain: Vec<_> = server_chain(message)?
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let name = ServerName::try_from(server_name).map_err(io::Error::other)?;
        self.0
            .verify_server_cert(&chain[0], &chain[1..], &name, &[], UnixTime::now())
            .map_err(io::Error::other)?;
        Ok(VerifiedServer {
            trust: self.clone(),
            leaf: chain[0].clone().into_owned(),
        })
    }
}

/// A PKI-verified leaf still requires CertificateVerify and Finished validation.
#[derive(Clone, Debug)]
pub struct VerifiedServer {
    trust: ServerTrust,
    leaf: CertificateDer<'static>,
}
impl VerifiedServer {
    pub fn verify_signature(&self, message: &[u8], transcript_hash: &[u8]) -> io::Result<()> {
        if message.len() < 8 || message[0] != 15 || u24(&message[1..4]) + 4 != message.len() {
            return Err(invalid());
        }
        let mut reader = Reader::init(&message[4..]);
        let signature = DigitallySignedStruct::read(&mut reader)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))?;
        if reader.any_left() {
            return Err(invalid());
        }
        let mut signed = vec![32; 64];
        signed.extend_from_slice(b"TLS 1.3, server CertificateVerify\0");
        signed.extend_from_slice(transcript_hash);
        self.trust
            .0
            .verify_tls13_signature(&signed, &self.leaf, &signature)
            .map(|_| ())
            .map_err(io::Error::other)
    }
}
