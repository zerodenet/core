//! Certificate-owned OCSP endpoints and RFC 6960 request encoding.
use std::io;
use x509_parser::{
    extensions::{GeneralName, ParsedExtension},
    prelude::*,
};

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn parse(bytes: &[u8]) -> io::Result<X509Certificate<'_>> {
    let (rest, certificate) = parse_x509_certificate(bytes).map_err(io::Error::other)?;
    if !rest.is_empty() {
        return Err(invalid("trailing issuer certificate data"));
    }
    Ok(certificate)
}
pub(super) fn endpoints(leaf: &[u8]) -> io::Result<(String, Option<String>)> {
    let leaf = parse(leaf)?;
    let mut responder = None;
    let mut issuer = None;
    for extension in leaf.extensions() {
        if let ParsedExtension::AuthorityInfoAccess(aia) = extension.parsed_extension() {
            for access in &aia.accessdescs {
                let GeneralName::URI(uri) = access.access_location else {
                    continue;
                };
                match access.access_method.to_id_string().as_str() {
                    "1.3.6.1.5.5.7.48.1" if responder.is_none() => responder = Some(uri.to_owned()),
                    "1.3.6.1.5.5.7.48.2" if issuer.is_none() => issuer = Some(uri.to_owned()),
                    _ => {}
                }
            }
        }
    }
    Ok((
        responder.ok_or_else(|| invalid("certificate has no OCSP responder"))?,
        issuer,
    ))
}
fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut result = vec![tag];
    if content.len() < 128 {
        result.push(content.len() as u8);
    } else {
        let bytes = content.len().to_be_bytes();
        let first = bytes.iter().position(|byte| *byte != 0).unwrap();
        result.push(0x80 | (bytes.len() - first) as u8);
        result.extend_from_slice(&bytes[first..]);
    }
    result.extend_from_slice(content);
    result
}
pub(super) fn encode(leaf: &[u8], issuer: &[u8]) -> io::Result<Vec<u8>> {
    let leaf = parse(leaf)?;
    let issuer = parse(issuer)?;
    if leaf.issuer() != issuer.subject() {
        return Err(invalid("OCSP issuer does not match certificate issuer"));
    }
    // Go's default OCSP request uses SHA-1 to identify the issuer (not to sign).
    let mut id = vec![0x30, 9, 6, 5, 0x2b, 0x0e, 3, 2, 0x1a, 5, 0];
    for value in [
        issuer.subject().as_raw(),
        issuer.public_key().subject_public_key.data.as_ref(),
    ] {
        let digest = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, value);
        id.extend(der(4, digest.as_ref()));
    }
    id.extend(der(2, leaf.raw_serial()));
    let mut result = der(0x30, &id);
    for _ in 0..4 {
        result = der(0x30, &result);
    }
    Ok(result)
}
