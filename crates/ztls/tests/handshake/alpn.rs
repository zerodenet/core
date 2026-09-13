#[test]
fn no_alpn_template_cannot_accept_configured_but_unsent_protocol() {
    let hello = crate::fingerprint::wire::build(
        &[1; 32],
        &[2; 32],
        "example.com",
        &[],
        &["h2"],
        crate::fingerprint::ClientHelloProfile::RandomizedNoAlpn,
        |_| Ok(Some(vec![0; 32])),
    )
    .unwrap();
    let offered = super::offered_alpn(&hello).unwrap();
    assert!(offered.is_empty());
    // EncryptedExtensions with a single ALPN selection: h2.
    let selected = [8, 0, 0, 11, 0, 9, 0, 16, 0, 5, 0, 3, 2, b'h', b'2'];
    assert!(super::alpn(&selected, &offered).is_err());
    assert_eq!(
        super::alpn(&selected, &["h2".into()]).unwrap(),
        Some(b"h2".to_vec())
    );
}
