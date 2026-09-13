//! Browser presentation on the mature TLS 1.2/1.3, PSK and ECH state machine.
use rustls::client::hello_profile::{ClientHelloProfile, ClientHelloProfileProvider};
use std::{io, sync::Arc};
use zero_traits::ClientTlsProfile;
use ztls::fingerprint::{wire, ClientHelloProfile as Preset};

#[derive(Debug)]
struct Presentation {
    preset: Preset,
    alpn: Vec<String>,
    groups: Vec<u16>,
}
impl ClientHelloProfileProvider for Presentation {
    fn profile(&self) -> Result<ClientHelloProfile, rustls::Error> {
        let alpn: Vec<_> = self.alpn.iter().map(String::as_str).collect();
        // The template carries only shape. Rustls replaces every live key share,
        // identity, ticket, binder, ALPN and encrypted ECH value before encoding.
        let hello = wire::build_with_options(
            &[0; 32],
            &[0; 32],
            "template.invalid",
            &[],
            &alpn,
            self.preset,
            &wire::ClientHelloOptions {
                supported_groups: self.groups.clone(),
                ..Default::default()
            },
            |_| Ok(Some(vec![0; 32])),
        )
        .map_err(|e| rustls::Error::General(e.to_string()))?;
        let (cipher_suites, extensions) =
            wire::parts(&hello).map_err(|e| rustls::Error::General(e.to_string()))?;
        Ok(ClientHelloProfile {
            cipher_suites,
            extensions,
        })
    }
}

pub(super) fn install(
    config: &mut rustls::ClientConfig,
    profile: &(impl ClientTlsProfile + ?Sized),
) -> io::Result<()> {
    let Some(name) = profile.client_fingerprint().filter(|name| *name != "none") else {
        return Ok(());
    };
    let preset = name.parse::<Preset>().map_err(super::config::invalid)?;
    let options = profile.tls_options();
    let groups = options
        .parameters
        .curve_preferences
        .iter()
        .map(|name| ztls::settings::curve(name).map_err(super::config::invalid))
        .collect::<io::Result<Vec<_>>>()?;
    if preset == Preset::RandomizedNoAlpn {
        config.alpn_protocols.clear();
    }
    if config.alpn_protocols.iter().any(|p| p == b"h2") {
        // Empty HTTP/2 ALPS means default settings. The connection preface
        // subsequently sends the carrier's configured SETTINGS normally.
        config
            .application_settings
            .insert(b"h2".to_vec(), Vec::new());
    }
    config.cert_decompressors = ztls::certificate::compression::RUSTLS_DECOMPRESSORS.to_vec();
    config.client_hello_profile = Some(Arc::new(Presentation {
        preset,
        alpn: if preset == Preset::RandomizedNoAlpn {
            Vec::new()
        } else {
            profile.alpn().to_vec()
        },
        groups,
    }));
    Ok(())
}

pub(super) fn configure_provider(
    provider: &mut rustls::crypto::CryptoProvider,
    preset: Preset,
) -> io::Result<()> {
    let (suites, extensions) = wire::preset_parts(preset)?;
    let mut available = rustls::crypto::aws_lc_rs::default_provider().cipher_suites;
    available.extend_from_slice(rustls::crypto::aws_lc_rs::legacy::CIPHER_SUITES);
    provider.cipher_suites = suites
        .iter()
        .filter_map(|id| {
            available
                .iter()
                .find(|suite| u16::from(suite.suite()) == *id)
                .copied()
        })
        .collect();
    let mut order = Vec::new();
    // Choose the initial live share from the capture, then retain every supported group.
    if let Some((_, share)) = extensions.iter().find(|(id, _)| *id == 51) {
        let mut r = ztls::buf_reader::BufReader::new(share);
        r.skip(2)?;
        while !r.is_consumed() {
            let id = r.read_u16_be()?;
            let length = r.read_u16_be()? as usize;
            r.skip(length)?;
            if !wire::is_grease(id) && !order.contains(&id) {
                order.push(id);
            }
        }
    }
    if let Some((_, groups)) = extensions.iter().find(|(id, _)| *id == 10) {
        for group in groups[2..].chunks_exact(2) {
            let id = u16::from_be_bytes([group[0], group[1]]);
            if !wire::is_grease(id) && !order.contains(&id) {
                order.push(id);
            }
        }
    }
    provider.kx_groups = order
        .into_iter()
        .filter_map(super::groups::lookup)
        .collect();
    if provider.cipher_suites.is_empty() || provider.kx_groups.is_empty() {
        return Err(super::config::invalid(
            "fingerprint has no usable TLS algorithms",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/tls/fingerprint_policy.rs"]
mod tests;
