#![cfg(feature = "crypto")]

use std::time::{SystemTime, UNIX_EPOCH};

use mieru::crypto::{derive_key, MieruCipher};
use mieru::metadata::{DataMetadata, DATA_SERVER_TO_CLIENT};
use mieru::segment::build_data_segment;
use mieru::session::MieruSession;
use mieru::traffic_pattern::TcpFragmentPattern;
use mieru::MieruOutbound;

#[test]
fn decrypt_server_data_waits_for_complete_segment() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs();
    let key = derive_key("user", "pass", now);
    let mut server_cipher = MieruCipher::new(&key);
    let mut outbound = MieruOutbound {
        mieru_session: MieruSession::new(),
        client_cipher: MieruCipher::new(&key),
        server_cipher: MieruCipher::with_nonce(&key, *server_cipher.current_nonce()),
        c2s_nonce_sent: true,
        s2c_nonce_recv: false,
        tcp_fragment: TcpFragmentPattern {
            enable: false,
            max_sleep_ms: 0,
        },
    };
    let payload = b"hello through mieru";
    let meta = DataMetadata {
        protocol_type: DATA_SERVER_TO_CLIENT,
        timestamp: MieruSession::timestamp_minutes(),
        session_id: outbound.mieru_session.session_id,
        sequence_number: 1,
        unack_sequence: 0,
        window_size: 1024,
        fragment_number: 0,
        prefix_length: 0,
        payload_length: payload.len() as u16,
        suffix_length: 0,
    };
    let wire = build_data_segment(&meta, payload, &mut server_cipher, true).unwrap();

    let partial = &wire[..wire.len() - 1];
    assert!(matches!(
        outbound.decrypt_server_data(partial),
        Err(zero_core::Error::Protocol("mieru: need more data"))
    ));

    let (parsed, consumed) = outbound.decrypt_server_data_with_consumed(&wire).unwrap();
    assert_eq!(parsed.payload, payload);
    assert_eq!(consumed, wire.len());
}

#[test]
fn decrypt_server_data_does_not_advance_cipher_on_incomplete_implicit_segment() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs();
    let key = derive_key("user", "pass", now);
    let mut server_cipher = MieruCipher::new(&key);
    let mut outbound = MieruOutbound {
        mieru_session: MieruSession::new(),
        client_cipher: MieruCipher::new(&key),
        server_cipher: MieruCipher::with_nonce(&key, *server_cipher.current_nonce()),
        c2s_nonce_sent: true,
        s2c_nonce_recv: false,
        tcp_fragment: TcpFragmentPattern {
            enable: false,
            max_sleep_ms: 0,
        },
    };

    let first = build_server_segment(
        &mut server_cipher,
        outbound.mieru_session.session_id,
        b"one",
        true,
    );
    let (parsed, consumed) = outbound.decrypt_server_data_with_consumed(&first).unwrap();
    assert_eq!(parsed.payload, b"one");
    assert_eq!(consumed, first.len());

    let second = build_server_segment(
        &mut server_cipher,
        outbound.mieru_session.session_id,
        b"two",
        false,
    );
    assert!(
        outbound
            .decrypt_server_data(&second[..second.len() - 1])
            .is_err(),
        "truncated segment should fail to decrypt"
    );

    let (parsed, consumed) = outbound.decrypt_server_data_with_consumed(&second).unwrap();
    assert_eq!(parsed.payload, b"two");
    assert_eq!(consumed, second.len());
}

fn build_server_segment(
    cipher: &mut MieruCipher,
    session_id: u32,
    payload: &[u8],
    include_nonce: bool,
) -> Vec<u8> {
    let meta = DataMetadata {
        protocol_type: DATA_SERVER_TO_CLIENT,
        timestamp: MieruSession::timestamp_minutes(),
        session_id,
        sequence_number: 1,
        unack_sequence: 0,
        window_size: 1024,
        fragment_number: 0,
        prefix_length: 0,
        payload_length: payload.len() as u16,
        suffix_length: 0,
    };
    build_data_segment(&meta, payload, cipher, include_nonce).unwrap()
}
