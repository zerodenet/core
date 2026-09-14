//! Explicit configuration overrides the browser template before key generation.
use super::{invalid, is_grease, vector};
use std::io;

#[derive(Clone, Debug, Default)]
pub struct ClientHelloOptions {
    /// Omit SNI only; certificate verification still uses the configured name.
    pub disable_sni: bool,
    /// Advertise only TLS 1.3 when no older handshake engine is available.
    pub tls13_only: bool,
    /// Empty preserves preset groups; otherwise ordered, validated group IDs.
    pub supported_groups: Vec<u16>,
}

pub(super) fn apply(
    ciphers: &mut Vec<u16>,
    extensions: &mut Vec<(u16, Vec<u8>)>,
    options: &ClientHelloOptions,
) -> io::Result<()> {
    if options.disable_sni {
        extensions.retain(|(kind, _)| *kind != 0);
    }
    if options.tls13_only {
        ciphers.retain(|id| is_grease(*id) || matches!(id, 0x1301..=0x1303));
        let (_, versions) = extensions
            .iter_mut()
            .find(|(k, _)| *k == 43)
            .ok_or_else(|| invalid("preset has no supported versions"))?;
        let mut offered = Vec::new();
        for id in versions
            .get(1..)
            .unwrap_or_default()
            .as_chunks::<2>()
            .0
            .iter()
        {
            let id = u16::from_be_bytes([id[0], id[1]]);
            if is_grease(id) || id == 0x0304 {
                offered.extend_from_slice(&id.to_be_bytes());
            }
        }
        if !offered.as_chunks::<2>().0.contains(&[3, 4]) {
            return Err(invalid("preset does not support TLS 1.3"));
        }
        *versions = vec![offered.len() as u8];
        versions.extend(offered);
    }
    if options.supported_groups.is_empty() {
        return Ok(());
    }
    let mut groups = Vec::new();
    for id in &options.supported_groups {
        if *id == 0 || is_grease(*id) {
            return Err(invalid("invalid explicit key exchange group"));
        }
        if !groups.contains(id) {
            groups.push(*id);
        }
    }
    let (_, supported) = extensions
        .iter_mut()
        .find(|(k, _)| *k == 10)
        .ok_or_else(|| invalid("preset has no supported groups"))?;
    *supported = vector(
        &groups
            .iter()
            .flat_map(|id| id.to_be_bytes())
            .collect::<Vec<_>>(),
    )?;
    // One real share for the most preferred group; HRR can select any remaining group.
    let (_, shares) = extensions
        .iter_mut()
        .find(|(k, _)| *k == 51)
        .ok_or_else(|| invalid("preset has no key share"))?;
    let mut share = groups[0].to_be_bytes().to_vec();
    share.extend_from_slice(&[0, 0]);
    *shares = vector(&share)?;
    Ok(())
}
