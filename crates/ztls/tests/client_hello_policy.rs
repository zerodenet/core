use ztls::{
    fingerprint::{
        wire::{parts, ClientHelloOptions},
        ClientHelloProfile,
    },
    handshake::{Tls13Config, Tls13Connection},
};
fn hello(config: Tls13Config) -> Vec<u8> {
    let mut connection = Tls13Connection::new(config).unwrap();
    let mut wire = Vec::new();
    connection.write_tls(&mut wire).unwrap();
    wire[5..].to_vec()
}
#[test]
fn generic_client_does_not_offer_unimplemented_tls_versions() {
    let hello = hello(Tls13Config {
        server_name: "example.com".into(),
        ..Default::default()
    });
    let (suites, extensions) = parts(&hello).unwrap();
    assert!(suites
        .iter()
        .all(|s| ztls::fingerprint::wire::is_grease(*s) || (0x1301..=0x1303).contains(s)));
    let versions = &extensions.iter().find(|(k, _)| *k == 43).unwrap().1;
    assert!(versions[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .all(|v| *v == [3, 4]
            || ztls::fingerprint::wire::is_grease(u16::from_be_bytes([v[0], v[1]]))));
}
#[test]
fn explicit_groups_and_sni_override_preset() {
    let hello = hello(Tls13Config {
        server_name: "verify.example.com".into(),
        client_hello_profile: ClientHelloProfile::Firefox148,
        client_hello_options: ClientHelloOptions {
            disable_sni: true,
            tls13_only: true,
            supported_groups: vec![23, 29, 23],
        },
        ..Default::default()
    });
    let (_, extensions) = parts(&hello).unwrap();
    assert!(!extensions.iter().any(|(k, _)| *k == 0));
    assert_eq!(
        extensions.iter().find(|(k, _)| *k == 10).unwrap().1,
        [0, 4, 0, 23, 0, 29]
    );
    let share = &extensions.iter().find(|(k, _)| *k == 51).unwrap().1;
    assert_eq!(&share[2..6], &[0, 23, 0, 65]);
    assert_eq!(share.len(), 71);
}
