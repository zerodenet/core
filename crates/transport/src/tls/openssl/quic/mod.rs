mod callbacks;
mod config;
mod ffi;
mod keys;
mod session;

pub(crate) use config::OpenSslQuicServerConfig;
pub use session::HandshakeData;

pub(crate) fn build_server_config(
    profile: &(impl zero_traits::ServerTlsProfile + ?Sized),
    base_dir: Option<&std::path::Path>,
    alpn_protocols: &[Vec<u8>],
) -> std::io::Result<Option<std::sync::Arc<dyn quinn_proto::crypto::ServerConfig>>> {
    if !super::use_openssl_server(profile)? {
        return Ok(None);
    }

    let mut profile = crate::profile::OwnedServerTlsProfile::from_profile(profile);
    profile.alpn = alpn_protocols
        .iter()
        .map(|protocol| {
            String::from_utf8(protocol.clone()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "OpenSSL QUIC ALPN contains non-UTF-8 bytes",
                )
            })
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let context = super::OpenSslServerContext::build(&profile, base_dir)?;
    Ok(Some(std::sync::Arc::new(OpenSslQuicServerConfig::new(
        std::sync::Arc::new(context),
    ))))
}

#[cfg(test)]
#[path = "../../../../tests/tls/openssl/quic.rs"]
mod tests;
