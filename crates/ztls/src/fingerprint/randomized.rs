// SPDX-License-Identifier: BSD-3-Clause
//! Randomized TLS 1.3 profile shape follows pinned uTLS/Xray weights.
//! Uses Rust OS-seeded randomness, not Go's seeded PRNG byte sequence.
use super::{catalog, wire::parts, ClientHelloProfile as P};
use rand::{seq::SliceRandom, Rng};
use std::sync::OnceLock;
type Shape = (Vec<u16>, Vec<(u16, Vec<u8>)>);
pub(super) fn shape(no_alpn: bool) -> Shape {
    static ALPN: OnceLock<Shape> = OnceLock::new();
    static NO_ALPN: OnceLock<Shape> = OnceLock::new();
    if no_alpn {
        NO_ALPN.get_or_init(|| generate(true))
    } else {
        ALPN.get_or_init(|| generate(false))
    }
    .clone()
}
fn vector(values: &[u16]) -> Vec<u8> {
    let mut b = ((values.len() * 2) as u16).to_be_bytes().to_vec();
    b.extend(values.iter().flat_map(|v| v.to_be_bytes()));
    b
}
fn generate(no_alpn: bool) -> Shape {
    let mut rng = rand::rng();
    let mut modern = vec![
        0xcca8, 0xcca9, 0xc02f, 0xc02b, 0xc030, 0xc02c, 0xc027, 0xc023, 0x009c, 0x009d, 0x003c,
    ];
    let mut legacy = vec![
        0xc013, 0xc009, 0xc014, 0xc00a, 0xc012, 0x002f, 0x0035, 0x000a,
    ];
    let mut ciphers = vec![0x1301, 0x1302, 0x1303];
    ciphers.shuffle(&mut rng);
    modern.shuffle(&mut rng);
    legacy.shuffle(&mut rng);
    ciphers.extend(modern);
    ciphers.extend(legacy);
    let total = ciphers.len() as f64;
    let mut i = 1;
    while i < ciphers.len() {
        if rng.random_bool(0.4 * i as f64 / total) {
            ciphers.remove(i);
        } else {
            i += 1;
        }
    }
    let mut signatures = vec![0x0403, 0x0401, 0x0503, 0x0501, 0x0201, 0x0601, 0x0804];
    if rng.random_bool(0.63) {
        signatures.push(0x0203);
    }
    if rng.random_bool(0.59) {
        signatures.push(0x0603);
    }
    if rng.random_bool(0.9) {
        signatures.extend([0x0805, 0x0806]);
    }
    signatures.shuffle(&mut rng);
    let hybrid = rng.random_bool(0.5);
    let mut groups = Vec::new();
    if hybrid || rng.random_bool(0.71) {
        groups.push(4588);
    }
    groups.extend([29, 23, 24]);
    if rng.random_bool(0.46) {
        groups.push(25);
    }
    let mut shares = Vec::new();
    if hybrid {
        shares.extend([0x11, 0xec, 0, 0]);
    }
    shares.extend([0, 29, 0, 0]);
    if rng.random_bool(0.5) {
        shares.extend([0, 23, 0, 0]);
    }
    let mut key_share = (shares.len() as u16).to_be_bytes().to_vec();
    key_share.extend(shares);
    let (_, source) = parts(catalog::preset(P::Chrome120).wire).expect("embedded uTLS capture");
    let mut extensions = Vec::new();
    for k in [0, 35, 11] {
        extensions.push(source.iter().find(|(id, _)| *id == k).unwrap().clone());
    }
    extensions.extend([
        (13, vector(&signatures)),
        (10, vector(&groups)),
        (51, key_share),
        (45, vec![1, 1]),
    ]);
    let versions = if rng.random_bool(0.5) {
        vec![8, 3, 4, 3, 3, 3, 2, 3, 1]
    } else {
        vec![4, 3, 4, 3, 3]
    };
    extensions.push((43, versions));
    for (k, p) in [(5, 0.74), (18, 0.46), (65281, 0.75), (23, 0.77)] {
        if rng.random_bool(p) {
            extensions.push(source.iter().find(|(id, _)| *id == k).unwrap().clone());
        }
    }
    if !no_alpn {
        extensions.push(source.iter().find(|(id, _)| *id == 16).unwrap().clone());
        if rng.random_bool(0.33) {
            extensions.push((17513, vec![0, 3, 2, b'h', b'2']));
        }
    }
    // uTLS also shuffles the padding position. The serializer inserts its body later.
    extensions.push((21, Vec::new()));
    extensions.shuffle(&mut rng);
    (ciphers, extensions)
}
