//! Immutable TLS configuration prepared from host-authorized CA material.
use std::{io, path::Path, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use zero_traits::ClientTlsProfile;

#[derive(Clone)]
pub struct PreparedClient {
    config: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
}
impl PreparedClient {
    /// The current prepared-material path supports the real Rustls policy only.
    /// OpenSSL and legacy TLS must retain their existing executor until its
    /// context can also consume authorized material without opening a path.
    pub fn validate_profile(profile: &(impl ClientTlsProfile + ?Sized)) -> io::Result<()> {
        if super::openssl::use_openssl_client(profile)? {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "prepared CA material is not integrated with OpenSSL TLS",
            ));
        }
        ztls::settings::validate_client(&profile.tls_options(), false)
            .map_err(super::config::invalid)
    }
    pub fn from_ca_pem(
        profile: &(impl ClientTlsProfile + ?Sized),
        base_dir: Option<&Path>,
        ca_pem: &[u8],
    ) -> io::Result<Self> {
        Self::validate_profile(profile)?;
        Ok(Self {
            config: Arc::new(super::config::client_with_ca_pem(
                profile, base_dir, false, ca_pem,
            )?),
            server_name: profile.server_name().map(str::to_owned),
        })
    }
    pub async fn connect<S>(
        &self,
        stream: S,
        default_server_name: &str,
    ) -> Result<zero_platform_tokio::TcpRelayStream, crate::RuntimeError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        super::connect_rustls_stream(
            stream,
            self.config.clone(),
            self.server_name.as_deref().unwrap_or(default_server_name),
        )
        .await
    }
}

#[cfg(test)]
#[path = "../../tests/tls/prepared.rs"]
mod tests;
