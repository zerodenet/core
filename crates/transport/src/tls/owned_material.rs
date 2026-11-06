//! Backend-preserving immutable TLS material for an actual resource owner.
//! Fixed PreparedClient library policy is unchanged.
use super::*;
use zero_traits::ClientTlsProfile;
#[derive(Clone)]
pub struct OwnedPreparedClient {
    backend: Backend,
    server_name: String,
}
#[derive(Clone)]
enum Backend {
    Rustls(Arc<rustls::ClientConfig>),
    OpenSsl(openssl::PreparedMaterial),
}
impl OwnedPreparedClient {
    pub fn validate_profile(profile: &(impl ClientTlsProfile + ?Sized)) -> io::Result<()> {
        if openssl::use_openssl_client(profile)? {
            openssl::PreparedMaterial::validate(profile)
        } else {
            PreparedClient::validate_profile(profile)
        }
    }
    /// The caller supplies only bytes returned by its declared File resource.
    /// This entry never opens ca_cert_path, including the OpenSSL branch.
    pub fn from_material(
        profile: &(impl ClientTlsProfile + ?Sized),
        base_dir: Option<&Path>,
        default_server_name: &str,
        ca_pem: Option<&[u8]>,
    ) -> io::Result<Self> {
        Self::validate_profile(profile)?;
        if profile.ca_cert_path().is_some() != ca_pem.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned TLS CA declaration and granted bytes differ",
            ));
        }
        let backend = if openssl::use_openssl_client(profile)? {
            Backend::OpenSsl(openssl::PreparedMaterial::new(
                profile,
                base_dir,
                default_server_name,
                ca_pem,
            )?)
        } else {
            Backend::Rustls(Arc::new(match ca_pem {
                Some(bytes) => config::client_with_ca_pem(profile, base_dir, false, bytes)?,
                None => config::client(profile, base_dir, false)?,
            }))
        };
        Ok(Self {
            backend,
            server_name: profile.server_name().unwrap_or(default_server_name).into(),
        })
    }
    pub async fn connect<S>(&self, stream: S) -> Result<TcpRelayStream, RuntimeError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        match &self.backend {
            Backend::Rustls(config) => {
                super::connect_rustls_stream(stream, config.clone(), &self.server_name).await
            }
            Backend::OpenSsl(prepared) => {
                let stream = prepared.connect(stream).await?;
                let control = stream.control();
                Ok(match control {
                    Some(control) => TcpRelayStream::with_transport_bypass_control(stream, control),
                    None => TcpRelayStream::new(stream),
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/tls/owned_material.rs"]
mod tests;
