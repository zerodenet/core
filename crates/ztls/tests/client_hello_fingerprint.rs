use ztls::fingerprint::{
    wire::{is_grease, parts},
    ClientHelloProfile as P,
};
use ztls::messages::construct_client_hello_with_profile;

fn hello(p: P) -> Vec<u8> {
    construct_client_hello_with_profile(
        &[0x11; 32],
        &[0x22; 32],
        &[0x33; 32],
        "example.com",
        &[],
        p.alpn_protocols(),
        p,
    )
    .unwrap()
}
fn normalize(h: &[u8], p: P) -> (Vec<u16>, Vec<(u16, Vec<u8>)>) {
    let (mut c, mut e) = parts(h).unwrap();
    for s in &mut c {
        if is_grease(*s) {
            *s = 0x0a0a;
        }
    }
    e.retain(|(k, _)| *k != 21);
    for (k, b) in &mut e {
        if is_grease(*k) {
            *k = 0x0a0a;
        }
        if *k == 10 || *k == 43 {
            let start = if *k == 10 { 2 } else { 1 };
            for v in b[start..].as_chunks_mut::<2>().0 {
                if is_grease(u16::from_be_bytes([v[0], v[1]])) {
                    v.copy_from_slice(&[10, 10]);
                }
            }
        }
        if *k == 51 {
            let mut i = 2;
            while i < b.len() {
                let g = u16::from_be_bytes([b[i], b[i + 1]]);
                let n = u16::from_be_bytes([b[i + 2], b[i + 3]]) as usize;
                if is_grease(g) {
                    b[i..i + 2].copy_from_slice(&[10, 10]);
                }
                b[i + 4..i + 4 + n].fill(0);
                i += 4 + n;
            }
        }
        if *k == 65037 {
            assert_eq!(b[0], 0);
            assert_eq!(&b[1..3], &[0, 1]);
            let cipher = u16::from_be_bytes([b[3], b[4]]);
            assert!(cipher == 1 || ((p == P::Firefox120 || p == P::Firefox148) && cipher == 3));
            assert_eq!(&b[6..8], &[0, 32]);
            let n = u16::from_be_bytes([b[40], b[41]]) as usize;
            assert_eq!(b.len(), 42 + n);
            if p == P::Firefox120 || p == P::Firefox148 {
                assert_eq!(n, 239);
            } else {
                assert!([144, 176, 208, 240].contains(&n));
            }
            *b = vec![0]; // independently validate the variable ECH envelope above
        }
    }
    if matches!(p, P::Chrome106 | P::Chrome120 | P::Chrome131 | P::Chrome133) {
        e.sort_by_key(|(k, _)| *k);
    }
    (c, e)
}
#[test]
fn every_preset_matches_the_pinned_utls_capture_after_ephemeral_normalization() {
    for &p in P::VERSIONED {
        let path = format!(
            "{}/src/fingerprint/presets/{}.bin",
            env!("CARGO_MANIFEST_DIR"),
            p
        );
        let reference = std::fs::read(path).unwrap();
        let actual = hello(p);
        assert_eq!(normalize(&actual, p), normalize(&reference, p), "{p}");
        assert_eq!(&actual[6..38], &[0x11; 32]);
        assert_eq!(&actual[39..71], &[0x22; 32]);
        if !parts(&reference)
            .unwrap()
            .1
            .iter()
            .any(|(k, _)| *k == 65037)
        {
            assert_eq!(actual.len(), reference.len(), "padding {p}");
        }
    }
}
#[test]
fn aliases_follow_the_fixed_official_defaults_and_versioned_names_roundtrip() {
    for (name, p) in [
        ("chrome", P::Chrome133),
        ("firefox", P::Firefox148),
        ("safari", P::Safari263),
        ("edge", P::Edge85),
        ("ios", P::Ios14),
        ("qq", P::Qq111),
    ] {
        assert_eq!(name.parse::<P>().unwrap(), p);
    }
    for &p in P::VERSIONED {
        assert_eq!(p.canonical_name().parse::<P>().unwrap(), p);
    }
    assert_eq!("hellosafari_26_3".parse::<P>().unwrap(), P::Safari263);
    assert!("unknown-browser".parse::<P>().is_err());
}
#[test]
fn entropy_is_fresh_and_firefox148_reuses_its_hybrid_component() {
    for p in [P::Chrome120, P::Chrome133, P::Firefox120, P::Firefox148] {
        let a = hello(p);
        let b = hello(p);
        assert_ne!(a, b);
        let shares = parts(&a)
            .unwrap()
            .1
            .into_iter()
            .find(|(k, _)| *k == 51)
            .unwrap()
            .1;
        let mut i = 2;
        let mut keys = std::collections::HashMap::new();
        while i < shares.len() {
            let g = u16::from_be_bytes([shares[i], shares[i + 1]]);
            let n = u16::from_be_bytes([shares[i + 2], shares[i + 3]]) as usize;
            keys.insert(g, shares[i + 4..i + 4 + n].to_vec());
            i += 4 + n;
        }
        if p == P::Firefox148 {
            assert_eq!(&keys[&4588][1184..], &keys[&29]);
        }
        if p == P::Chrome133 {
            assert_ne!(&keys[&4588][1184..], &keys[&29]);
        }
    }
}
#[test]
fn random_modes_and_input_bounds_are_enforced() {
    for name in ["127.0.0.1", "[::1]", "[fe80::1%en0]"] {
        let dotted_hello = construct_client_hello_with_profile(
            &[0; 32],
            &[0; 32],
            &[0; 32],
            name,
            &[],
            &["h2"],
            P::Chrome120,
        )
        .unwrap();
        assert!(!parts(&dotted_hello)
            .unwrap()
            .1
            .iter()
            .any(|(kind, _)| *kind == 0));
    }
    let dotted_hello = construct_client_hello_with_profile(
        &[0; 32],
        &[0; 32],
        &[0; 32],
        "example.com.",
        &[],
        &["h2"],
        P::Chrome120,
    )
    .unwrap();
    let sni = parts(&dotted_hello)
        .unwrap()
        .1
        .into_iter()
        .find(|(kind, _)| *kind == 0)
        .unwrap()
        .1;
    assert!(sni.ends_with(b"example.com"));

    for p in [P::Randomized, P::RandomizedNoAlpn] {
        assert_eq!(
            normalize(&hello(p), p),
            normalize(&hello(p), p),
            "process-stable random shape"
        );
        let (suites, extensions) = parts(&hello(p)).unwrap();
        assert!(matches!(suites[0], 0x1301..=0x1303));
        assert!(!extensions
            .iter()
            .any(|(kind, _)| is_grease(*kind) || *kind == 65037));
    }

    let a = ztls::fingerprint::wire::resolve(P::Random);
    assert_eq!(a, ztls::fingerprint::wire::resolve(P::Random));
    assert!(P::RANDOM_CANDIDATES.contains(&a));
    assert!(!parts(&hello(P::RandomizedNoAlpn))
        .unwrap()
        .1
        .iter()
        .any(|(k, _)| [16, 17513, 17613].contains(k)));
    assert!(construct_client_hello_with_profile(
        &[0; 32],
        &[0; 32],
        &[0; 32],
        "example.com",
        &[],
        &[&"a".repeat(256)],
        P::Chrome120
    )
    .is_err());
}
