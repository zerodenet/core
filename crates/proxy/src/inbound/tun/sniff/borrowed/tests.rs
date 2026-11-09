use super::super::{normalize_sniffed_domain, sniff_tls_handshake, SniffOutcome};
use super::*;
fn handshake(ech: bool, truncated_extension: bool) -> Vec<u8> {
    let name = b"Mixed.Example";
    let mut extensions = vec![0, 0];
    extensions.extend_from_slice(&((name.len() + 5) as u16).to_be_bytes());
    extensions.extend_from_slice(&((name.len() + 3) as u16).to_be_bytes());
    extensions.push(0);
    extensions.extend_from_slice(&(name.len() as u16).to_be_bytes());
    extensions.extend_from_slice(name);
    if ech {
        extensions.extend_from_slice(&[0xfe, 0x0d, 0, 0]);
    }
    if truncated_extension {
        extensions.extend_from_slice(&[0x12, 0x34, 0, 20, 1, 2]);
    }
    let mut body = vec![0; 34];
    body.extend_from_slice(&[0, 0, 0, 0]);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);
    let n = body.len();
    let mut hello = vec![1, (n >> 16) as u8, (n >> 8) as u8, n as u8];
    hello.extend_from_slice(&body);
    hello
}
fn original_name(bytes: &[u8]) -> Option<String> {
    match sniff_tls_handshake(bytes) {
        Ok(SniffOutcome::Domain { domain, .. }) => normalize_sniffed_domain(domain),
        _ => None,
    }
}
#[test]
fn borrowed_tun_sni_keeps_original_ech_and_malformed_extension_tolerance() {
    for (ech, truncated) in [(false, false), (false, true), (true, false), (true, true)] {
        let hello = handshake(ech, truncated);
        let name = sniff_tls_name(&hello);
        assert_eq!(
            name.as_ref().map(InlineName::as_str),
            original_name(&hello).as_deref()
        );
        assert_eq!(name.is_none(), ech);
        if let Some(name) = name {
            assert_eq!(name.as_str(), "mixed.example");
        }
    }
}
#[test]
fn incomplete_original_handshake_never_borrows_an_unparsed_name() {
    let hello = handshake(false, false);
    for length in 0..hello.len() {
        assert_eq!(
            sniff_tls_name(&hello[..length])
                .as_ref()
                .map(InlineName::as_str),
            original_name(&hello[..length]).as_deref(),
            "prefix {length}"
        );
    }
}
