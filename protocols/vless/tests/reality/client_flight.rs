use super::*;
use crate::reality::reality_server_connection::{RealityServerConfig, RealityServerConnection};

fn flight() -> (
    RealityClientConnection,
    CipherSuite,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
) {
    let secret = [7; 32];
    let public = PublicKey::from(&StaticSecret::from(secret));
    let mut client = RealityClientConnection::new(RealityClientConfig {
        public_key: *public.as_bytes(),
        server_name: "localhost".into(),
        ..Default::default()
    })
    .unwrap();
    let mut server = RealityServerConnection::new(RealityServerConfig {
        private_key: secret,
        short_ids: vec![[0; 8]],
        ..Default::default()
    });
    let mut wire = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut wire).unwrap();
    }
    server.read_tls(&mut &wire[..]).unwrap();
    server.process_new_packets().unwrap();
    wire.clear();
    while server.wants_write() {
        server.write_tls(&mut wire).unwrap();
    }
    let hello_end = 5 + u16::from_be_bytes([wire[3], wire[4]]) as usize;
    client.read_tls(&mut &wire[..hello_end]).unwrap();
    client.process_new_packets().unwrap();
    let HandshakeState::ProcessingHandshake {
        server_handshake_traffic_secret,
        cipher_suite,
        ..
    } = &client.handshake_state
    else {
        panic!("missing server hello");
    };
    let suite = *cipher_suite;
    let (key, iv) = derive_traffic_keys(server_handshake_traffic_secret, suite).unwrap();
    let mut cursor = hello_end;
    let mut seq = 0;
    let mut plaintext = Vec::new();
    while cursor < wire.len() {
        let size = u16::from_be_bytes([wire[cursor + 3], wire[cursor + 4]]) as usize;
        if wire[cursor] == CONTENT_TYPE_APPLICATION_DATA {
            plaintext.extend(
                decrypt_handshake_message(
                    suite,
                    &key,
                    &iv,
                    seq,
                    &wire[cursor + 5..cursor + 5 + size],
                    size as u16,
                )
                .unwrap(),
            );
            seq += 1;
        }
        cursor += size + 5;
    }
    (client, suite, key, iv, plaintext)
}

#[test]
fn fragmented_encrypted_messages_resume_at_incomplete_header_and_body() {
    let (mut client, suite, key, iv, plaintext) = flight();
    let key = AeadKey::new(suite, &key).unwrap();
    let mut seq = 0;
    // Two-byte records split every four-byte handshake header as well as bodies.
    for chunk in plaintext.chunks(2) {
        let mut wire = Vec::new();
        RecordEncryptor::new(&key, &iv, &mut seq)
            .encrypt_handshake(chunk, &mut wire)
            .unwrap();
        for part in wire.chunks(3) {
            client.read_tls(&mut &part[..]).unwrap();
            client.process_new_packets().unwrap();
        }
    }
    assert!(!client.is_handshaking());
}

#[test]
fn invalid_finished_and_handshake_order_are_rejected_even_with_valid_record_authentication() {
    for change_order in [false, true] {
        let (mut client, suite, key, iv, mut plaintext) = flight();
        if change_order {
            plaintext[0] = HANDSHAKE_TYPE_CERTIFICATE;
        } else {
            *plaintext.last_mut().unwrap() ^= 1;
        }
        let key = AeadKey::new(suite, &key).unwrap();
        let mut seq = 0;
        let mut wire = Vec::new();
        RecordEncryptor::new(&key, &iv, &mut seq)
            .encrypt_handshake(&plaintext, &mut wire)
            .unwrap();
        client.read_tls(&mut &wire[..]).unwrap();
        let error = client
            .process_new_packets()
            .expect_err("malformed handshake accepted");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(client.is_handshaking());
    }
}
