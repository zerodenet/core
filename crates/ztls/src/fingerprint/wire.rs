//! Materialize pinned uTLS wire captures, replacing every ephemeral value.
//! Captures are data, never live keys or reusable ECH payloads.
use super::{catalog, ClientHelloProfile};
use crate::buf_reader::BufReader;
use rand::{seq::SliceRandom, Rng, RngCore};
use std::io;

mod policy;
pub use policy::ClientHelloOptions;

pub fn is_grease(value: u16) -> bool {
    value & 0x0f0f == 0x0a0a && value >> 8 == value & 255
}
fn grease(rng: &mut impl Rng) -> u16 {
    0x0a0a + 0x1010 * rng.random_range(0..16)
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn vector(bytes: &[u8]) -> io::Result<Vec<u8>> {
    let len = u16::try_from(bytes.len()).map_err(|_| invalid("ClientHello vector exceeds u16"))?;
    let mut out = len.to_be_bytes().to_vec();
    out.extend_from_slice(bytes);
    Ok(out)
}
/// Parsed handshake vectors, also used by wire-level regression tests.
pub type HelloParts = (Vec<u16>, Vec<(u16, Vec<u8>)>);
pub fn parts(hello: &[u8]) -> io::Result<HelloParts> {
    let mut r = BufReader::new(hello);
    r.skip(38)?;
    let sid = r.read_u8()? as usize;
    r.skip(sid)?;
    let n = r.read_u16_be()? as usize;
    let suites = r
        .read_slice(n)?
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u16::from_be_bytes([v[0], v[1]]))
        .collect();
    let n = r.read_u8()? as usize;
    r.skip(n)?;
    let n = r.read_u16_be()? as usize;
    let mut e = BufReader::new(r.read_slice(n)?);
    if !r.is_consumed() {
        return Err(invalid("trailing ClientHello bytes"));
    }
    let mut extensions = Vec::new();
    while !e.is_consumed() {
        let kind = e.read_u16_be()?;
        let n = e.read_u16_be()? as usize;
        extensions.push((kind, e.read_slice(n)?.to_vec()));
    }
    Ok((suites, extensions))
}

pub fn resolve(profile: ClientHelloProfile) -> ClientHelloProfile {
    use ClientHelloProfile::*;
    match profile {
        Edge120 => Chrome120,
        Random => {
            static PICK: std::sync::OnceLock<ClientHelloProfile> = std::sync::OnceLock::new();
            *PICK.get_or_init(|| {
                ClientHelloProfile::RANDOM_CANDIDATES
                    [rand::rng().random_range(0..ClientHelloProfile::RANDOM_CANDIDATES.len())]
            })
        }
        Randomized | RandomizedNoAlpn => Chrome120,
        p => p,
    }
}

/// `key_share` must retain the corresponding private state until ServerHello.
/// Returning None disables an explicitly configured optional hybrid share.
#[allow(clippy::too_many_arguments)]
pub fn build(
    random: &[u8; 32],
    session: &[u8; 32],
    server_name: &str,
    suites: &[u16],
    alpn: &[&str],
    profile: ClientHelloProfile,
    key_share: impl FnMut(u16) -> io::Result<Option<Vec<u8>>>,
) -> io::Result<Vec<u8>> {
    build_with_options(
        random,
        session,
        server_name,
        suites,
        alpn,
        profile,
        &ClientHelloOptions::default(),
        key_share,
    )
}

/// Apply explicit TLS policy before materializing ephemeral key shares.
#[allow(clippy::too_many_arguments)]
pub fn build_with_options(
    random: &[u8; 32],
    session: &[u8; 32],
    server_name: &str,
    suites: &[u16],
    alpn: &[&str],
    profile: ClientHelloProfile,
    options: &ClientHelloOptions,
    mut key_share: impl FnMut(u16) -> io::Result<Option<Vec<u8>>>,
) -> io::Result<Vec<u8>> {
    let server_name = server_name.trim_end_matches('.');
    let host = server_name
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(server_name);
    let host = host
        .rsplit_once('%')
        .filter(|(host, _)| !host.is_empty())
        .map_or(host, |(host, _)| host);
    let omit_sni = host.parse::<std::net::IpAddr>().is_ok();
    if server_name.is_empty() || server_name.len() > 253 || !server_name.is_ascii() {
        return Err(invalid("invalid ClientHello server name"));
    }
    if alpn.iter().any(|p| p.is_empty() || p.len() > 255) {
        return Err(invalid("invalid ClientHello ALPN length"));
    }
    let preset = catalog::preset(resolve(profile));
    let randomized = matches!(
        profile,
        ClientHelloProfile::Randomized | ClientHelloProfile::RandomizedNoAlpn
    );
    let (mut ciphers, mut extensions) = if randomized {
        super::randomized::shape(profile == ClientHelloProfile::RandomizedNoAlpn)
    } else {
        parts(preset.wire)?
    };
    policy::apply(&mut ciphers, &mut extensions, options)?;
    let mut rng = rand::rng();
    let g_cipher = grease(&mut rng);
    let g_group = grease(&mut rng);
    let g_version = grease(&mut rng);
    let g_ext = grease(&mut rng);
    let mut g_last = grease(&mut rng);
    if g_last == g_ext {
        g_last = 0x0a0a + (((g_last >> 12) + 1) % 16) * 0x1010;
    }
    // Preserve all upstream legacy suites; caller overrides only the TLS 1.3 offer.
    if !suites.is_empty() {
        if suites.iter().any(|s| !matches!(s, 0x1301..=0x1303)) {
            return Err(invalid("custom TLS only negotiates TLS 1.3 suites"));
        }
        let first = ciphers
            .iter()
            .position(|s| matches!(s, 0x1301..=0x1303))
            .ok_or_else(|| invalid("preset has no TLS 1.3"))?;
        ciphers.retain(|s| !matches!(s, 0x1301..=0x1303));
        ciphers.splice(first..first, suites.iter().copied());
    }
    for s in &mut ciphers {
        if is_grease(*s) {
            *s = g_cipher;
        }
    }
    let mut omitted = Vec::new();
    for (kind, body) in &mut extensions {
        if *kind != 51 {
            continue;
        }
        let mut r = BufReader::new(body);
        r.skip(2)?;
        let mut shares = Vec::new();
        while !r.is_consumed() {
            let group = r.read_u16_be()?;
            let n = r.read_u16_be()? as usize;
            r.skip(n)?;
            let (group, data) = if is_grease(group) {
                (g_group, vec![0])
            } else if let Some(data) = key_share(group)? {
                (group, data)
            } else {
                omitted.push(group);
                continue;
            };
            shares.extend_from_slice(&group.to_be_bytes());
            shares.extend(vector(&data)?);
        }
        *body = vector(&shares)?;
    }
    let mut first_grease = true;
    for (kind, body) in &mut extensions {
        if is_grease(*kind) {
            *kind = if first_grease { g_ext } else { g_last };
            first_grease = false;
            continue;
        }
        match *kind {
            0 => {
                let mut name = vec![0];
                name.extend(vector(server_name.as_bytes())?);
                *body = vector(&name)?;
            }
            10 => {
                let mut groups = Vec::new();
                for bytes in body[2..].as_chunks::<2>().0 {
                    let group = u16::from_be_bytes([bytes[0], bytes[1]]);
                    if omitted.contains(&group) {
                        continue;
                    }
                    groups.extend_from_slice(
                        &if is_grease(group) { g_group } else { group }.to_be_bytes(),
                    );
                }
                *body = vector(&groups)?;
            }
            43 => {
                for bytes in body[1..].as_chunks_mut::<2>().0 {
                    if is_grease(u16::from_be_bytes([bytes[0], bytes[1]])) {
                        bytes.copy_from_slice(&g_version.to_be_bytes());
                    }
                }
            }
            16 => {
                let mut protocols = Vec::new();
                for p in alpn {
                    protocols.push(p.len() as u8);
                    protocols.extend_from_slice(p.as_bytes());
                }
                *body = vector(&protocols)?;
            }
            65037 => {
                let cipher = preset.ech_ciphers[rng.random_range(0..preset.ech_ciphers.len())];
                let len =
                    preset.ech_lengths[rng.random_range(0..preset.ech_lengths.len())] as usize + 16;
                *body = vec![0];
                body.extend_from_slice(&cipher.0.to_be_bytes());
                body.extend_from_slice(&cipher.1.to_be_bytes());
                body.push(rng.random());
                // ECH GREASE encapsulates an ephemeral X25519 public key.
                let private = x25519_dalek::StaticSecret::from(rng.random::<[u8; 32]>());
                body.extend(vector(x25519_dalek::PublicKey::from(&private).as_bytes())?);
                let mut payload = vec![0; len];
                rng.fill_bytes(&mut payload);
                body.extend(vector(&payload)?);
            }
            _ => {}
        }
    }
    extensions.retain(|(k, _)| {
        (*k != 21 || randomized)
            && (*k != 0 || !omit_sni)
            && !((*k == 16 || *k == 17513 || *k == 17613)
                && (alpn.is_empty() || profile == ClientHelloProfile::RandomizedNoAlpn))
    });
    if preset.shuffle && !randomized {
        // uTLS leaves the two GREASE sentinels, padding and PSK in place.
        let indices: Vec<_> = extensions
            .iter()
            .enumerate()
            .filter(|(_, (k, _))| !is_grease(*k) && *k != 41)
            .map(|(i, _)| i)
            .collect();
        let mut values: Vec<_> = indices.iter().map(|i| extensions[*i].clone()).collect();
        values.shuffle(&mut rng);
        for (i, value) in indices.into_iter().zip(values) {
            extensions[i] = value;
        }
    }
    let mut hello = vec![1, 0, 0, 0, 3, 3];
    hello.extend_from_slice(random);
    hello.push(32);
    hello.extend_from_slice(session);
    let cipher_bytes: Vec<_> = ciphers.iter().flat_map(|s| s.to_be_bytes()).collect();
    hello.extend(vector(&cipher_bytes)?);
    hello.extend_from_slice(&[1, 0]);
    let mut encoded = Vec::new();
    let mut padding_position = None;
    for (kind, body) in extensions {
        if kind == 21 {
            padding_position = Some(encoded.len());
            continue;
        }
        encoded.extend_from_slice(&kind.to_be_bytes());
        encoded.extend(vector(&body)?);
    }
    let unpadded = hello.len() + 2 + encoded.len();
    if (preset.padding || randomized) && unpadded > 255 && unpadded < 512 {
        let len = if unpadded < 508 {
            512 - unpadded - 4
        } else {
            1
        };
        let mut padding = vec![0, 21];
        padding.extend(vector(&vec![0; len])?);
        let position = padding_position.unwrap_or(encoded.len());
        encoded.splice(position..position, padding);
    }
    hello.extend(vector(&encoded)?);
    if hello.len() > 16384 {
        return Err(invalid("ClientHello exceeds one TLS plaintext record"));
    }
    let n = hello.len() - 4;
    hello[1..4].copy_from_slice(&(n as u32).to_be_bytes()[1..]);
    Ok(hello)
}

/// Fixed source shape for selecting real provider algorithms before ClientHello creation.
pub fn preset_parts(profile: ClientHelloProfile) -> io::Result<HelloParts> {
    parts(catalog::preset(resolve(profile)).wire)
}
