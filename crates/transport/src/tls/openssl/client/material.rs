//! The same OpenSSL policy/context from actual granted CA bytes, never a path.
use super::*;
use crate::{
    profile::OwnedClientTlsProfile,
    tls::cache::{OpenSslClientCacheKey, OpenSslClientConfig},
};
#[derive(Clone)]
pub(crate) struct PreparedMaterial {
    cached: OpenSslClientConfig,
    profile: OwnedClientTlsProfile,
    server_name: String,
}
impl PreparedMaterial {
    pub(crate) fn validate(profile: &(impl ClientTlsProfile + ?Sized)) -> io::Result<()> {
        let options = profile.tls_options();
        ztls::settings::validate_client(&options, false).map_err(io::Error::other)?;
        if options.ech.source().map_err(io::Error::other)?.is_some() {
            return Err(invalid(
                "OpenSSL TLS does not support ECH client configuration",
            ));
        }
        if profile.client_fingerprint().is_some() {
            return Err(invalid(
                "OpenSSL TLS does not apply rustls ClientHello fingerprint presets",
            ));
        }
        Ok(())
    }
    pub(crate) fn new(
        profile: &(impl ClientTlsProfile + ?Sized),
        base_dir: Option<&Path>,
        default_server_name: &str,
        ca_pem: Option<&[u8]>,
    ) -> io::Result<Self> {
        Self::validate(profile)?;
        let options = profile.tls_options();
        let server_name = profile.server_name().unwrap_or(default_server_name);
        if profile.ca_cert_path().is_some() != ca_pem.is_some() {
            return Err(invalid(
                "owned OpenSSL CA path and granted material must match",
            ));
        }
        let mut ca_hash = ring::digest::Context::new(&ring::digest::SHA256);
        if let Some(bytes) = ca_pem {
            ca_hash.update(&(bytes.len() as u64).to_be_bytes());
            ca_hash.update(bytes);
        }
        let digest = ca_hash.finish();
        let cache_key = options.parameters.enable_session_resumption.then(|| {
            OpenSslClientCacheKey::new(
                OwnedClientTlsProfile::from_profile(profile),
                base_dir.map(Path::to_path_buf),
                server_name.into(),
                digest.as_ref().try_into().expect("SHA-256 digest length"),
            )
        });
        let cached = if let Some(cached) =
            cache_key.as_ref().and_then(crate::tls::cache::get_openssl)
        {
            cached
        } else {
            let mut builder =
                SslContextBuilder::new(SslMethod::tls_client()).map_err(io::Error::other)?;
            super::super::parameters::apply(&mut builder, &options.parameters)?;
            builder.set_read_ahead(false);
            let pins = pin::decode(&options.pinned_peer_cert_sha256)?;
            if profile.insecure() && pins.is_empty() && options.verify_peer_names.is_empty() {
                builder.set_verify(SslVerifyMode::NONE);
            } else {
                roots::configure(&mut builder, options.disable_system_roots)?;
                if let Some(bytes) = ca_pem {
                    let certs = openssl::x509::X509::stack_from_pem(bytes)
                        .map_err(|source| io::Error::new(io::ErrorKind::InvalidInput, source))?;
                    if certs.is_empty() {
                        return Err(invalid("granted OpenSSL CA PEM contains no certificates"));
                    }
                    for cert in certs {
                        builder
                            .cert_store_mut()
                            .add_cert(cert)
                            .map_err(io::Error::other)?;
                    }
                }
                if pins.is_empty() {
                    builder.set_verify(SslVerifyMode::PEER);
                } else {
                    builder.set_verify_callback(
                        SslVerifyMode::PEER,
                        move |preverified, context| {
                            preverified || pin::verification_override(context, &pins)
                        },
                    );
                }
            }
            let session = std::sync::Arc::new(std::sync::Mutex::new(None));
            if options.parameters.enable_session_resumption {
                builder.set_session_cache_mode(SslSessionCacheMode::CLIENT);
                let sessions = session.clone();
                builder.set_new_session_callback(move |_ssl, value| {
                    *sessions.lock().unwrap_or_else(|error| error.into_inner()) = Some(value);
                });
            } else {
                builder.set_session_cache_mode(SslSessionCacheMode::OFF);
            }
            let cached = OpenSslClientConfig {
                context: builder.build(),
                session,
            };
            match cache_key {
                Some(key) => crate::tls::cache::insert_openssl(key, cached),
                None => cached,
            }
        };
        Ok(Self {
            cached,
            profile: OwnedClientTlsProfile::from_profile(profile),
            server_name: server_name.into(),
        })
    }
    pub(crate) async fn connect<S>(
        &self,
        socket: S,
    ) -> io::Result<super::super::stream::OpenSslTlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        super::handshake(socket, &self.profile, &self.cached, &self.server_name).await
    }
}
