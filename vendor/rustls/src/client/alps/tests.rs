use super::*;
use alloc::vec;
use crate::sync::Arc;
use crate::{
    client::hello_profile::ClientHelloProfile, common_state::Side, crypto::aws_lc_rs,
    hash_hs::HandshakeHashBuffer, msgs::handshake::ProtocolName, RootCertStore,
};
fn config() -> ClientConfig {
    let mut config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
        .application_settings
        .insert(b"h2".to_vec(), vec![1, 2, 3]);
    config
}
fn hello(codepoint: u16) -> ClientHelloDetails {
    let mut hello = ClientHelloDetails::new(vec![ProtocolName::from(b"h2".to_vec())], 0);
    hello.wire_profile = Some(ClientHelloProfile {
        cipher_suites: Vec::new(),
        extensions: vec![(codepoint, vec![0, 3, 2, b'h', b'2'])],
    });
    hello
}
#[test]
fn both_codepoints_consume_peer_settings_and_authenticate_the_client_extension() {
    for codepoint in [17513, 17613] {
        let config = config();
        let mut common = CommonState::new(Side::Client);
        common.alpn_protocol = Some(ProtocolName::from(b"h2".to_vec()));
        let mut exts = ServerExtensions::default();
        if codepoint == 17513 {
            exts.application_settings_old = Some(Payload::new(&b"peer"[..]));
        } else {
            exts.application_settings = Some(Payload::new(&b"peer"[..]));
        }
        receive(&mut common, &config, &hello(codepoint), &exts, None).unwrap();
        assert_eq!(common.alps.as_ref().unwrap().peer, b"peer");
        assert!(common.peer_application_settings().is_none());
        let hash = &aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .unwrap()
            .common
            .hash_provider;
        let mut transcript = HandshakeHashBuffer::new().start_hash(*hash);
        let before = transcript.current_hash();
        {
            let mut flight = HandshakeFlightTls13::new(&mut transcript);
            emit(&common, &mut flight);
        }
        assert_ne!(before.as_ref(), transcript.current_hash().as_ref());
        common.may_send_application_data = true;
        common.may_receive_application_data = true;
        assert_eq!(common.peer_application_settings(), Some(&b"peer"[..]));
    }
}
#[test]
fn early_data_reuses_only_identical_settings_and_does_not_emit_an_extension() {
    let config = config();
    let hello = hello(17613);
    let saved = Negotiated {
        codepoint: 17613,
        protocol: b"h2".to_vec(),
        local: vec![1, 2, 3],
        peer: b"peer".to_vec(),
        send_extension: true,
    };
    assert!(early_compatible(
        &config,
        hello.wire_profile.as_ref(),
        Some(&saved)
    ));
    let mut changed = config.clone();
    changed
        .application_settings
        .get_mut(&b"h2"[..])
        .unwrap()
        .push(4);
    assert!(!early_compatible(
        &changed,
        hello.wire_profile.as_ref(),
        Some(&saved)
    ));
    let mut common = CommonState::new(Side::Client);
    common.alpn_protocol = Some(ProtocolName::from(b"h2".to_vec()));
    common.early_traffic = true;
    let exts = ServerExtensions {
        early_data_ack: Some(()),
        ..Default::default()
    };
    receive(&mut common, &config, &hello, &exts, Some(&saved)).unwrap();
    assert!(!common.alps.as_ref().unwrap().send_extension);
}
#[test]
fn rejects_unsolicited_protocol_and_dual_codepoints() {
    let config = config();
    for both in [false, true] {
        let mut common = CommonState::new(Side::Client);
        let exts = ServerExtensions {
            application_settings: Some(Payload::new(&b""[..])),
            application_settings_old: both.then(|| Payload::new(&b""[..])),
            ..Default::default()
        };
        assert!(receive(&mut common, &config, &hello(17613), &exts, None).is_err());
    }
}
