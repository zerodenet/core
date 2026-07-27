use ztls::fingerprint::ClientHelloProfile;
use ztls::messages::construct_client_hello_with_profile;

fn client_hello(profile: ClientHelloProfile) -> Vec<u8> {
    construct_client_hello_with_profile(
        &[0x11; 32],
        &[0x22; 32],
        &[0x33; 32],
        "example.com",
        profile.cipher_suites(),
        profile.alpn_protocols(),
        profile,
    )
    .expect("ClientHello")
}

fn extension_types(hello: &[u8]) -> Vec<u16> {
    let cipher_len = u16::from_be_bytes([hello[71], hello[72]]) as usize;
    let compression_offset = 73 + cipher_len;
    let compression_len = hello[compression_offset] as usize;
    let extensions_len_offset = compression_offset + 1 + compression_len;
    let extensions_len = u16::from_be_bytes([
        hello[extensions_len_offset],
        hello[extensions_len_offset + 1],
    ]) as usize;
    let mut offset = extensions_len_offset + 2;
    let end = offset + extensions_len;
    let mut types = Vec::new();
    while offset < end {
        let extension_type = u16::from_be_bytes([hello[offset], hello[offset + 1]]);
        let body_len = u16::from_be_bytes([hello[offset + 2], hello[offset + 3]]) as usize;
        types.push(extension_type);
        offset += 4 + body_len;
    }
    assert_eq!(offset, end);
    types
}

fn extension_body(hello: &[u8], expected_type: u16) -> Vec<u8> {
    let cipher_len = u16::from_be_bytes([hello[71], hello[72]]) as usize;
    let compression_offset = 73 + cipher_len;
    let compression_len = hello[compression_offset] as usize;
    let extensions_len_offset = compression_offset + 1 + compression_len;
    let extensions_len = u16::from_be_bytes([
        hello[extensions_len_offset],
        hello[extensions_len_offset + 1],
    ]) as usize;
    let mut offset = extensions_len_offset + 2;
    let end = offset + extensions_len;
    while offset < end {
        let extension_type = u16::from_be_bytes([hello[offset], hello[offset + 1]]);
        let body_len = u16::from_be_bytes([hello[offset + 2], hello[offset + 3]]) as usize;
        if extension_type == expected_type {
            return hello[offset + 4..offset + 4 + body_len].to_vec();
        }
        offset += 4 + body_len;
    }
    panic!("missing extension {expected_type}");
}

#[test]
fn aliases_are_pinned_to_versioned_profiles() {
    assert_eq!(
        "chrome".parse::<ClientHelloProfile>().unwrap(),
        ClientHelloProfile::Chrome120
    );
    assert_eq!(
        "firefox".parse::<ClientHelloProfile>().unwrap(),
        ClientHelloProfile::Firefox120
    );
    assert_eq!(
        "safari".parse::<ClientHelloProfile>().unwrap(),
        ClientHelloProfile::Safari160
    );
    assert_eq!(
        "edge".parse::<ClientHelloProfile>().unwrap(),
        ClientHelloProfile::Edge120
    );
    assert!("random-browser".parse::<ClientHelloProfile>().is_err());
}

#[test]
fn browser_profiles_have_distinct_extension_orders() {
    let chrome = extension_types(&client_hello(ClientHelloProfile::Chrome120));
    let firefox = extension_types(&client_hello(ClientHelloProfile::Firefox120));
    let safari = extension_types(&client_hello(ClientHelloProfile::Safari160));

    assert_eq!(
        &chrome[..12],
        &[0, 23, 11, 10, 51, 13, 50, 16, 27, 22, 45, 43]
    );
    assert_eq!(
        &firefox[..12],
        &[0, 23, 10, 11, 13, 50, 16, 43, 51, 45, 27, 22]
    );
    assert_eq!(
        &safari[..12],
        &[0, 11, 10, 13, 50, 23, 16, 43, 51, 45, 27, 22]
    );
    assert_eq!(chrome.last(), Some(&21));
    assert_ne!(chrome, firefox);
    assert_ne!(firefox, safari);
}

#[test]
fn firefox_profile_changes_tls13_cipher_preference() {
    let hello = client_hello(ClientHelloProfile::Firefox120);
    let cipher_len = u16::from_be_bytes([hello[71], hello[72]]) as usize;
    assert_eq!(cipher_len, 6);
    assert_eq!(&hello[73..79], &[0x13, 0x01, 0x13, 0x03, 0x13, 0x02]);
}

#[test]
fn safari_profile_changes_supported_group_preference() {
    let chrome = client_hello(ClientHelloProfile::Chrome120);
    let safari = client_hello(ClientHelloProfile::Safari160);

    assert_eq!(
        extension_body(&chrome, 10),
        [0, 6, 0, 0x1d, 0, 0x17, 0, 0x18]
    );
    assert_eq!(
        extension_body(&safari, 10),
        [0, 6, 0, 0x17, 0, 0x1d, 0, 0x18]
    );
    assert_eq!(&extension_body(&safari, 51)[2..4], &[0, 0x1d]);
}
