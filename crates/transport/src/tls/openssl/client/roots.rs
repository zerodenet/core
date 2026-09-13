use std::io;

use openssl::{ssl::SslContextBuilder, x509::X509};

pub(super) fn configure(
    builder: &mut SslContextBuilder,
    disable_system_roots: bool,
) -> io::Result<()> {
    if disable_system_roots {
        return Ok(());
    }

    for certificate in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
        let certificate = X509::from_der(certificate.as_ref()).map_err(io::Error::other)?;
        builder
            .cert_store_mut()
            .add_cert(certificate)
            .map_err(io::Error::other)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../../tests/tls/openssl/roots.rs"]
mod tests;
