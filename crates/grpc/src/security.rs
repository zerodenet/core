use std::net::SocketAddr;
use std::sync::Arc;

use tonic::Request;

#[derive(Clone)]
pub struct GrpcServerAuth {
    bearer_token: Arc<str>,
}

impl GrpcServerAuth {
    pub fn single_admin(bearer_token: String) -> Self {
        Self {
            bearer_token: Arc::from(bearer_token),
        }
    }

    pub(super) fn is_authorized<T>(&self, request: &Request<T>) -> bool {
        let supplied = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        supplied == Some(self.bearer_token.as_ref())
    }
}

#[derive(Clone)]
pub struct GrpcServerTls {
    cert_pem: Arc<[u8]>,
    key_pem: Arc<[u8]>,
    client_ca_cert_pem: Option<Arc<[u8]>>,
}

impl GrpcServerTls {
    pub fn new(cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self {
            cert_pem: Arc::from(cert_pem),
            key_pem: Arc::from(key_pem),
            client_ca_cert_pem: None,
        }
    }

    pub fn with_client_ca(mut self, client_ca_cert_pem: Vec<u8>) -> Self {
        self.client_ca_cert_pem = Some(Arc::from(client_ca_cert_pem));
        self
    }

    pub fn requires_client_certificate(&self) -> bool {
        self.client_ca_cert_pem.is_some()
    }

    pub(super) fn native_acceptor(
        &self,
    ) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certs =
            rustls_pemfile::certs(&mut self.cert_pem.as_ref()).collect::<Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut self.key_pem.as_ref())?.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gRPC TLS private key is missing",
            )
        })?;
        let builder = rustls::ServerConfig::builder();
        let mut config = if let Some(ca) = &self.client_ca_cert_pem {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut ca.as_ref()) {
                roots.add(cert?)?;
            }
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        } else {
            builder.with_no_client_auth().with_single_cert(certs, key)?
        };
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
    }
}

#[derive(Clone, Default)]
pub struct GrpcServerSecurity {
    pub auth: Option<GrpcServerAuth>,
    pub tls: Option<GrpcServerTls>,
    pub allow_insecure_remote: bool,
}

impl GrpcServerSecurity {
    pub(super) fn validate_for(&self, addr: SocketAddr) -> Result<(), std::io::Error> {
        if !addr.ip().is_loopback() && self.tls.is_none() && !self.allow_insecure_remote {
            return Err(std::io::Error::other(
                "plaintext gRPC on a non-loopback listener requires explicit insecure opt-in or native TLS",
            ));
        }
        let has_client_identity = self
            .tls
            .as_ref()
            .is_some_and(GrpcServerTls::requires_client_certificate);
        if !addr.ip().is_loopback() && self.auth.is_none() && !has_client_identity {
            return Err(std::io::Error::other(
                "remote gRPC requires bearer authentication or mTLS client authentication",
            ));
        }
        Ok(())
    }
}
