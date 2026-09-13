use std::io;

use base64::Engine;
use openssl::{
    sha::sha256,
    stack::{Stack, StackRef},
    x509::{
        store::X509StoreBuilder,
        verify::{X509VerifyFlags, X509VerifyParam},
        X509PurposeId, X509Ref, X509StoreContext, X509StoreContextRef, X509,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Match {
    None,
    Leaf,
    Authority,
}

pub(super) fn decode(pins: &[String]) -> io::Result<Vec<[u8; 32]>> {
    pins.iter()
        .map(|pin| {
            base64::engine::general_purpose::STANDARD
                .decode(pin)
                .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(pin))
                .map_err(|_| invalid("TLS certificate pin must be base64 SHA-256"))?
                .try_into()
                .map_err(|_| invalid("TLS certificate pin must contain 32 bytes"))
        })
        .collect()
}

pub(super) fn verification_override(context: &mut X509StoreContextRef, pins: &[[u8; 32]]) -> bool {
    let Some(chain) = context.chain() else {
        return false;
    };
    let Some(leaf) = chain.get(0) else {
        return false;
    };
    if is_pinned(leaf, pins) {
        // Match Xray's leaf-pin contract: possession of the pinned leaf key is
        // sufficient, including when the ordinary trust path is unavailable.
        return true;
    }
    let security_level = X509StoreContext::ssl_idx()
        .ok()
        .and_then(|index| context.ex_data(index))
        .map(|ssl| ssl.security_level())
        .unwrap_or(1)
        .max(1);
    chain
        .iter()
        .skip(1)
        .filter(|certificate| is_pinned(certificate, pins) && is_ca(certificate))
        .any(|authority| verify_with_authority(leaf, chain, authority, security_level))
}

fn verify_with_authority(
    leaf: &X509Ref,
    chain: &StackRef<X509>,
    authority: &X509Ref,
    security_level: u32,
) -> bool {
    let Ok(mut store) = X509StoreBuilder::new() else {
        return false;
    };
    let Ok(mut parameters) = X509VerifyParam::new() else {
        return false;
    };
    parameters.set_auth_level(security_level.min(i32::MAX as u32) as i32);
    if parameters.set_purpose(X509PurposeId::SSL_SERVER).is_err()
        || store.set_flags(X509VerifyFlags::PARTIAL_CHAIN).is_err()
        || store.set_param(&parameters).is_err()
        || store.add_cert(authority.to_owned()).is_err()
    {
        return false;
    }
    let store = store.build();
    let Ok(mut intermediates) = Stack::new() else {
        return false;
    };
    let Ok(leaf_der) = leaf.to_der() else {
        return false;
    };
    for certificate in chain.iter() {
        if certificate
            .to_der()
            .is_ok_and(|certificate_der| certificate_der == leaf_der)
        {
            continue;
        }
        if intermediates.push(certificate.to_owned()).is_err() {
            return false;
        }
    }
    let Ok(mut verification) = X509StoreContext::new() else {
        return false;
    };
    verification
        .init(&store, leaf, &intermediates, |context| {
            context.verify_cert()
        })
        .is_ok_and(|verified| verified)
}

pub(super) fn verify<S>(
    stream: &tokio_openssl::SslStream<S>,
    pins: &[String],
) -> io::Result<Match> {
    let expected = decode(pins)?;
    if expected.is_empty() {
        return Ok(Match::None);
    }
    let leaf = stream
        .ssl()
        .peer_certificate()
        .ok_or_else(|| invalid("TLS peer sent no certificate"))?;
    if is_pinned(&leaf, &expected) {
        return Ok(Match::Leaf);
    }
    let security_level = stream.ssl().security_level().max(1);
    if stream.ssl().peer_cert_chain().is_some_and(|chain| {
        chain.iter().any(|authority| {
            is_ca(authority)
                && is_pinned(authority, &expected)
                && verify_with_authority(&leaf, chain, authority, security_level)
        })
    }) {
        Ok(Match::Authority)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "TLS peer certificate chain does not match a configured leaf or CA pin",
        ))
    }
}

fn is_pinned(certificate: &X509Ref, pins: &[[u8; 32]]) -> bool {
    certificate
        .to_der()
        .ok()
        .is_some_and(|der| pins.iter().any(|pin| pin == &sha256(&der)))
}

fn is_ca(certificate: &X509Ref) -> bool {
    certificate.to_der().ok().is_some_and(|der| {
        x509_parser::parse_x509_certificate(&der).is_ok_and(|(_, parsed)| parsed.is_ca())
    })
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
