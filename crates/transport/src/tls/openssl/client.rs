use std::{ffi::CString, io, net::IpAddr, path::Path, pin::Pin};

use foreign_types::ForeignTypeRef;
use openssl::{
    ssl::{Ssl, SslContextBuilder, SslMethod, SslSessionCacheMode, SslVerifyMode},
    x509::{verify::X509CheckFlags, X509Ref},
};
use tokio::io::{AsyncRead, AsyncWrite};
use zero_traits::{ClientTlsProfile, TlsBackend, TransportBypassControl};

use crate::tls::{certificates::resolve_path, record_boundary::TlsRecordBoundary};

mod pin;
mod roots;

pub(crate) fn use_openssl_client(profile: &(impl ClientTlsProfile + ?Sized)) -> io::Result<bool> {
    let options = profile.tls_options();
    let ech = options.ech.source().map_err(io::Error::other)?.is_some();
    let required =
        ztls::settings::needs_openssl_parameters(&options.parameters).map_err(io::Error::other)?;
    match options.backend {
        TlsBackend::Auto => Ok(!ech && required),
        TlsBackend::Rustls => Ok(false),
        TlsBackend::OpenSsl => Ok(true),
    }
}

pub(crate) async fn connect<S, P>(
    socket: S,
    profile: &P,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> io::Result<super::stream::OpenSslTlsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    P: ClientTlsProfile + ?Sized,
{
    let options = profile.tls_options();
    let server_name = profile.server_name().unwrap_or(default_server_name);
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
    let ca_path = profile
        .ca_cert_path()
        .map(|path| resolve_path(base_dir, path));
    let mut ca_hash = ring::digest::Context::new(&ring::digest::SHA256);
    if let Some(path) = &ca_path {
        let bytes = std::fs::read(path)?;
        ca_hash.update(&(bytes.len() as u64).to_be_bytes());
        ca_hash.update(&bytes);
    }
    let ca_digest = ca_hash.finish();
    let cache_key = options.parameters.enable_session_resumption.then(|| {
        crate::tls::cache::OpenSslClientCacheKey::new(
            crate::profile::OwnedClientTlsProfile::from_profile(profile),
            base_dir.map(Path::to_path_buf),
            server_name.to_owned(),
            ca_digest
                .as_ref()
                .try_into()
                .expect("SHA-256 digest length"),
        )
    });
    let cached = if let Some(cached) = cache_key.as_ref().and_then(crate::tls::cache::get_openssl) {
        cached
    } else {
        let mut builder =
            SslContextBuilder::new(SslMethod::tls_client()).map_err(io::Error::other)?;
        super::parameters::apply(&mut builder, &options.parameters)?;
        builder.set_read_ahead(false);
        let pins = pin::decode(&options.pinned_peer_cert_sha256)?;
        if profile.insecure() && pins.is_empty() && options.verify_peer_names.is_empty() {
            builder.set_verify(SslVerifyMode::NONE);
        } else {
            roots::configure(&mut builder, options.disable_system_roots)?;
            if let Some(path) = &ca_path {
                builder.set_ca_file(path).map_err(io::Error::other)?;
            }
            if pins.is_empty() {
                builder.set_verify(SslVerifyMode::PEER);
            } else {
                builder.set_verify_callback(SslVerifyMode::PEER, move |preverified, context| {
                    preverified || pin::verification_override(context, &pins)
                });
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
        let cached = crate::tls::cache::OpenSslClientConfig {
            context: builder.build(),
            session,
        };
        match cache_key.clone() {
            Some(key) => crate::tls::cache::insert_openssl(key, cached),
            None => cached,
        }
    };
    let mut ssl = Ssl::new(&cached.context).map_err(io::Error::other)?;
    if let Some(session) = cached
        .session
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
    {
        unsafe { ssl.set_session(&session) }.map_err(io::Error::other)?;
    }
    if !profile.disable_sni() {
        ssl.set_hostname(server_name).map_err(io::Error::other)?;
    }
    if !profile.insecure() && options.pinned_peer_cert_sha256.is_empty() {
        if options.verify_peer_names.len() <= 1 {
            let verify_name = options
                .verify_peer_names
                .first()
                .map(String::as_str)
                .unwrap_or(server_name);
            if let Ok(ip) = verify_name.parse::<IpAddr>() {
                ssl.param_mut().set_ip(ip).map_err(io::Error::other)?;
            } else {
                ssl.param_mut().set_hostflags(
                    X509CheckFlags::NEVER_CHECK_SUBJECT | X509CheckFlags::NO_PARTIAL_WILDCARDS,
                );
                ssl.param_mut()
                    .set_host(verify_name)
                    .map_err(io::Error::other)?;
            }
        }
    }
    let protocols = encode_alpn(profile.alpn())?;
    if !protocols.is_empty() {
        ssl.set_alpn_protos(&protocols).map_err(io::Error::other)?;
    }
    let control = TransportBypassControl::default();
    let mut stream = tokio_openssl::SslStream::new(
        ssl,
        TlsRecordBoundary::with_control(socket, control.clone()),
    )
    .map_err(io::Error::other)?;
    Pin::new(&mut stream)
        .connect()
        .await
        .map_err(io::Error::other)?;
    let pin_match = pin::verify(&stream, &options.pinned_peer_cert_sha256)?;
    if pin_match == pin::Match::Authority {
        let names = if options.verify_peer_names.is_empty() {
            vec![server_name.to_owned()]
        } else {
            options.verify_peer_names.clone()
        };
        verify_any_name(&stream, &names)?;
    } else if pin_match == pin::Match::None
        && !options.verify_peer_names.is_empty()
        && (profile.insecure() || options.verify_peer_names.len() > 1)
    {
        verify_any_name(&stream, &options.verify_peer_names)?;
    }
    let control = super::stream::is_tls13(&stream).then_some(control);
    Ok(super::stream::OpenSslTlsStream {
        stream,
        control,
        failed: false,
    })
}

fn verify_any_name<S>(stream: &tokio_openssl::SslStream<S>, names: &[String]) -> io::Result<()> {
    let certificate = stream
        .ssl()
        .peer_certificate()
        .ok_or_else(|| invalid("TLS peer sent no certificate"))?;
    if names
        .iter()
        .any(|name| certificate_matches(&certificate, name))
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "TLS peer certificate does not match any configured verification name",
        ))
    }
}

fn certificate_matches(certificate: &X509Ref, name: &str) -> bool {
    if let Ok(ip) = name.parse::<IpAddr>() {
        let expected = match ip {
            IpAddr::V4(ip) => ip.octets().to_vec(),
            IpAddr::V6(ip) => ip.octets().to_vec(),
        };
        return unsafe {
            openssl_sys::X509_check_ip(
                certificate.as_ptr(),
                expected.as_ptr(),
                expected.len(),
                openssl_sys::X509_CHECK_FLAG_NEVER_CHECK_SUBJECT,
            ) == 1
        };
    }
    let Ok(name) = CString::new(name) else {
        return false;
    };
    unsafe {
        openssl_sys::X509_check_host(
            certificate.as_ptr(),
            name.as_ptr(),
            0,
            openssl_sys::X509_CHECK_FLAG_NEVER_CHECK_SUBJECT
                | openssl_sys::X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS,
            std::ptr::null_mut(),
        ) == 1
    }
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

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
