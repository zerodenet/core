//! Connection-local ClientHello presentation applied before PSK binders and ECH.
//! Profiles cannot replace identities, credentials, live key shares or verification.
use crate::msgs::codec::{Codec, Reader};
use crate::msgs::enums::ExtensionType;
use crate::msgs::handshake::{ClientExtensions, ClientHelloPayload, KeyShareEntry};
use crate::{CipherSuite, Error};
use alloc::{vec, vec::Vec};
use core::fmt::Debug;

/// A freshly generated presentation, reused for this connection's HelloRetryRequest.
#[derive(Clone, Debug)]
pub struct ClientHelloProfile {
    /// Preferred cipher IDs. Unsupported non-GREASE suites are never advertised.
    pub cipher_suites: Vec<u16>,
    /// Ordered extension templates. Security-sensitive values are supplied by rustls.
    pub extensions: Vec<(u16, Vec<u8>)>,
}
/// Generates a presentation for each connection, never a cached ephemeral ClientHello.
pub trait ClientHelloProfileProvider: Debug + Send + Sync {
    /// Generate extension order and fresh GREASE values for one connection.
    fn profile(&self) -> Result<ClientHelloProfile, Error>;
}
fn grease(id: u16) -> bool {
    id & 0x0f0f == 0x0a0a && id >> 8 == id & 255
}
fn invalid() -> Error {
    Error::General("invalid ClientHello profile".into())
}

pub(super) fn apply(
    hello: &mut ClientHelloPayload,
    profile: &ClientHelloProfile,
    retry_group: bool,
    managed_ech: bool,
    settings: &alloc::collections::BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<(), Error> {
    let mut bytes = Vec::new();
    for (id, body) in &profile.extensions {
        if body.len() > u16::MAX as usize {
            return Err(invalid());
        }
        id.encode(&mut bytes);
        (body.len() as u16).encode(&mut bytes);
        bytes.extend_from_slice(body);
    }
    if bytes.len() > u16::MAX as usize {
        return Err(invalid());
    }
    let mut encoded = (bytes.len() as u16).to_be_bytes().to_vec();
    encoded.extend(bytes);
    let template = ClientExtensions::read(&mut Reader::init(&encoded))?;
    let available = hello.cipher_suites.clone();
    let mut suites = Vec::new();
    for id in &profile.cipher_suites {
        let suite = CipherSuite::from(*id);
        if (grease(*id) || available.contains(&suite)) && !suites.contains(&suite) {
            suites.push(suite);
        }
    }
    for suite in available {
        if suite != CipherSuite::TLS_EMPTY_RENEGOTIATION_INFO_SCSV && !suites.contains(&suite) {
            suites.push(suite);
        }
    }
    hello.cipher_suites = suites;
    hello.wire_order = Some(
        profile
            .extensions
            .iter()
            .map(|(id, _)| ExtensionType::from(*id))
            .collect(),
    );
    if let (Some(preferred), Some(available)) = (&template.named_groups, &mut hello.named_groups) {
        let mut ordered: Vec<_> = preferred
            .iter()
            .copied()
            .filter(|id| grease(u16::from(*id)) || available.contains(id))
            .collect();
        for id in available.iter() {
            if !ordered.contains(id) {
                ordered.push(*id);
            }
        }
        *available = ordered;
    }
    if let (Some(preferred), Some(available)) =
        (&template.signature_schemes, &mut hello.signature_schemes)
    {
        let ordered: Vec<_> = preferred
            .iter()
            .copied()
            .filter(|id| available.contains(id))
            .collect();
        if !ordered.is_empty() {
            *available = ordered;
        }
    }
    if let Some(available) = &mut hello.certificate_compression_algorithms {
        let ordered: Vec<_> = template
            .certificate_compression_algorithms
            .as_deref()
            .unwrap_or_default()
            .iter()
            .copied()
            .filter(|id| available.contains(id))
            .collect();
        hello.certificate_compression_algorithms = (!ordered.is_empty()).then_some(ordered);
    }
    if let Some(shares) = &mut hello.key_shares {
        if !retry_group {
            let mut ordered = Vec::new();
            for requested in template.key_shares.as_deref().unwrap_or_default() {
                if grease(u16::from(requested.group)) {
                    ordered.push(KeyShareEntry::new(requested.group, &[0]));
                } else if let Some(live) =
                    shares.iter().find(|entry| entry.group == requested.group)
                {
                    ordered.push(live.clone());
                }
            }
            // Retain the actual selected share (including a cached group hint).
            for share in shares.iter() {
                if !ordered.iter().any(|entry| entry.group == share.group) {
                    ordered.push(share.clone());
                }
            }
            *shares = ordered;
        }
    }
    if template.renegotiation_info.is_some() {
        hello.renegotiation_info = template.renegotiation_info;
    }
    if !managed_ech && hello.encrypted_client_hello.is_none() {
        hello.encrypted_client_hello = template.encrypted_client_hello;
    }
    // Only static, non-credential extensions may be supplied as opaque data.
    // All key shares, tickets, binders, SNI, ALPN and ECH remain typed/live values.
    for (id, body) in &profile.extensions {
        if grease(*id) || matches!(*id, 18 | 21 | 17513 | 17613) {
            if matches!(*id, 17513 | 17613) && hello.protocols.is_none() {
                continue;
            }
            hello.wire_extra.push((*id, body.clone()));
        }
        if *id == 43 {
            if let Some(versions) = &hello.supported_versions {
                let mut selected = Vec::new();
                for value in body.get(1..).unwrap_or_default().chunks_exact(2) {
                    let id = u16::from_be_bytes([value[0], value[1]]);
                    if grease(id)
                        || (id == 0x0304 && versions.tls13)
                        || (id == 0x0303 && versions.tls12)
                    {
                        selected.extend_from_slice(value);
                    }
                }
                // Add any enabled version absent from an old template.
                for (enabled, id) in [(versions.tls13, 0x0304u16), (versions.tls12, 0x0303)] {
                    if enabled && !selected.chunks_exact(2).any(|v| v == id.to_be_bytes()) {
                        selected.extend_from_slice(&id.to_be_bytes());
                    }
                }
                let mut body = vec![selected.len() as u8];
                body.extend(selected);
                hello.wire_extra.push((43, body));
            }
        }
    }
    // These values must never be supplied by the opaque profile extension list.
    debug_assert!(!hello
        .wire_extra
        .iter()
        .any(|(id, _)| matches!(id, 0 | 16 | 35 | 41 | 42 | 44 | 51 | 65037)));
    let alpn = hello.protocols.clone();
    let tls13 = hello.supported_versions.as_ref().is_some_and(|v| v.tls13);
    hello.wire_extra.retain_mut(|(id, body)| {
        if !matches!(*id, 17513 | 17613) {
            return true;
        }
        if !tls13 {
            return false;
        }
        let Some(protocols) = super::alps::protocols(body) else {
            return false;
        };
        let offered: Vec<_> = protocols
            .into_iter()
            .filter(|p| {
                settings.contains_key(*p)
                    && alpn
                        .as_ref()
                        .is_some_and(|list| list.iter().any(|v| v.as_ref() == *p))
            })
            .collect();
        if offered.is_empty() {
            return false;
        }
        let mut encoded = Vec::new();
        for p in offered {
            encoded.push(p.len() as u8);
            encoded.extend_from_slice(p);
        }
        *body = (encoded.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(&encoded);
        true
    });
    Ok(())
}
