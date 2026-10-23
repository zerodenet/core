use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};

use rustls::pki_types::PrivateKeyDer;

pub(super) fn load_certs(
    path: &Path,
) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = File::open(path).map_err(|source| {
        io::Error::new(
            source.kind(),
            format!(
                "failed to read tls certificate `{}`: {source}",
                path.display()
            ),
        )
    })?;
    parse_certs(BufReader::new(file), path)
}

/// Parse already-authorized material; the path is diagnostic only.
pub(super) fn parse_certs(
    mut reader: impl BufRead,
    path: &Path,
) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "tls certificate `{}` contains no certificates",
                path.display()
            ),
        ));
    }
    Ok(certs)
}

pub(super) fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|source| {
        io::Error::new(
            source.kind(),
            format!(
                "failed to read tls private key `{}`: {source}",
                path.display()
            ),
        )
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "tls private key `{}` contains no private key",
                path.display()
            ),
        )
    })
}

pub(super) fn resolve_path(base_dir: Option<&Path>, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        return path;
    }
    base_dir
        .map(|base_dir| base_dir.join(&path))
        .unwrap_or(path)
}
