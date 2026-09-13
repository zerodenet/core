use super::*;
use crate::reality::mldsa;

const SEED: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

#[test]
fn mldsa65_public_key_matches_pinned_xray_command() {
    // Xray-core v26.3.27 / d2758a023cd7f4174a5a5fa4ff66e487d4342ba0:
    // xray mldsa65 -i AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
    assert_eq!(
        mldsa::public_key(SEED).unwrap(),
        include_str!("../fixtures/reality_mldsa65_zero_seed.txt").trim()
    );
}

#[test]
fn mldsa_certificate_binds_authentication_both_hellos_and_ed25519_key() {
    let signer = mldsa::signer(SEED).unwrap();
    let verifier = mldsa::verifier(&mldsa::public_key(SEED).unwrap()).unwrap();
    let auth = [42; 32];
    let hellos = b"client hello followed by server hello";
    let (certificate, _) = construct_reality_certificate(&auth, Some(&signer), hellos).unwrap();
    let (_, parsed) = x509_parser::parse_x509_certificate(&certificate).unwrap();
    assert_eq!(parsed.extensions().len(), 1);
    assert_eq!(parsed.extensions()[0].oid.as_bytes(), &[0]);
    assert_eq!(parsed.extensions()[0].value.len(), 3309);
    assert!(parsed.subject().iter().next().is_none());
    mldsa::verify(&verifier, &certificate, &auth, hellos).unwrap();
    assert!(mldsa::verify(&verifier, &certificate, &[43; 32], hellos).is_err());
    assert!(mldsa::verify(&verifier, &certificate, &auth, b"different hello").is_err());
    let mut changed = certificate.clone();
    let proof = parsed.extensions()[0].value;
    let offset = proof.as_ptr() as usize - certificate.as_ptr() as usize;
    changed[offset + 32] ^= 1;
    assert!(mldsa::verify(&verifier, &changed, &auth, hellos).is_err());
    let (no_proof, _) = construct_reality_certificate(&auth, None, hellos).unwrap();
    assert!(mldsa::verify(&verifier, &no_proof, &auth, hellos).is_err());
}

#[test]
fn authenticated_fragmented_hello_uses_target_random_record_size_and_mldsa_proof() {
    use crate::reality::{
        hello,
        reality_client_connection::{RealityClientConfig, RealityClientConnection},
        target::Shape,
    };
    for group in [29, 4588] {
        let secret = [7; 32];
        let public = x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(secret));
        let verifying = mldsa::verifier(&mldsa::public_key(SEED).unwrap()).unwrap();
        let mut client = RealityClientConnection::new(RealityClientConfig {
            public_key: *public.as_bytes(),
            short_id: [0; 8],
            server_name: "localhost".into(),
            mldsa65: Some(verifying),
            ..Default::default()
        })
        .unwrap();
        let mut request = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut request).unwrap();
        }
        let canonical = hello::assembled(&request).unwrap().unwrap().0;
        let sid = ztls::util::extract_session_id_slice(&canonical).unwrap();
        let mut target_hello =
            ztls::messages::construct_server_hello(&[19; 32], sid, 0x1301, &[6; 32]).unwrap();
        if group == 4588 {
            let end = target_hello.len() - 36;
            target_hello.truncate(end);
            target_hello[end - 2..].copy_from_slice(&1124u16.to_be_bytes());
            target_hello.extend_from_slice(&4588u16.to_be_bytes());
            target_hello.extend_from_slice(&1120u16.to_be_bytes());
            target_hello.resize(target_hello.len() + 1120, 0);
            let ext = 4 + 2 + 32 + 1 + sid.len() + 2 + 1;
            target_hello[ext..ext + 2].copy_from_slice(&1134u16.to_be_bytes());
            let size = target_hello.len() - 4;
            target_hello[1..4].copy_from_slice(&(size as u32).to_be_bytes()[1..]);
        }
        let mut target = ztls::messages::write_record_header(22, target_hello.len() as u16);
        target.extend(target_hello);
        target.extend([20, 3, 3, 0, 1, 1]);
        target.extend([23, 3, 3, 0x17, 0x6b]); // 5995 encrypted bytes, 6000 with header.
        let shape = Shape::inspect(&target).unwrap().unwrap();
        let config = RealityServerConfig {
            private_key: secret,
            short_ids: vec![[0; 8]],
            mldsa65: Some(mldsa::signer(SEED).unwrap()),
            ..Default::default()
        };
        let mut server = RealityServerConnection::new(config).with_target_shape(shape);
        // Fragmentation changes only records, never authenticated ClientHello bytes.
        for chunk in canonical[5..].chunks(7) {
            let mut record = ztls::messages::write_record_header(22, chunk.len() as u16);
            record.extend_from_slice(chunk);
            for byte in record {
                server.read_tls(&mut &[byte][..]).unwrap();
                server.process_new_packets().unwrap();
            }
        }
        let mut response = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut response).unwrap();
        }
        assert_eq!(&response[11..43], &[19; 32]);
        let first = 5 + u16::from_be_bytes([response[3], response[4]]) as usize;
        assert_eq!(&response[first..first + 6], &[20, 3, 3, 0, 1, 1]);
        assert_eq!(response.len() - first - 6, 6000);
        client.read_tls(&mut &response[..]).unwrap();
        client.process_new_packets().unwrap();
        let mut finish = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut finish).unwrap();
        }
        server.read_tls(&mut &finish[..]).unwrap();
        server.process_new_packets().unwrap();
        assert!(!server.is_handshaking());
        assert!(!client.is_handshaking());
    }
}

#[test]
fn authenticated_handshake_reproduces_target_post_handshake_record_lengths() {
    use crate::reality::{
        hello,
        reality_client_connection::{RealityClientConfig, RealityClientConnection},
        target::Shape,
    };
    let secret = [7; 32];
    let public = x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(secret));
    let mut client = RealityClientConnection::new(RealityClientConfig {
        public_key: *public.as_bytes(),
        short_id: [0; 8],
        server_name: "localhost".into(),
        ..Default::default()
    })
    .unwrap();
    let mut request = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut request).unwrap();
    }
    let canonical = hello::assembled(&request).unwrap().unwrap().0;
    let session_id = ztls::util::extract_session_id_slice(&canonical).unwrap();
    let target_hello =
        ztls::messages::construct_server_hello(&[19; 32], session_id, 0x1301, &[6; 32]).unwrap();
    let mut target = ztls::messages::write_record_header(22, target_hello.len() as u16);
    target.extend(target_hello);
    target.extend([20, 3, 3, 0, 1, 1]);
    target.extend([23, 3, 3, 0x17, 0x6b]);
    let shape = Shape::inspect(&target)
        .unwrap()
        .unwrap()
        .with_detection(vec![64, 96], 1);
    let mut ccs_limited = RealityServerConnection::new(RealityServerConfig {
        private_key: secret,
        short_ids: vec![[0; 8]],
        ..Default::default()
    })
    .with_target_shape(shape.clone());
    ccs_limited.read_tls(&mut &request[..]).unwrap();
    ccs_limited.process_new_packets().unwrap();
    let mut ignored = Vec::new();
    while ccs_limited.wants_write() {
        ccs_limited.write_tls(&mut ignored).unwrap();
    }
    ccs_limited.read_tls(&mut &[20, 3, 3, 0, 1, 1][..]).unwrap();
    ccs_limited.process_new_packets().unwrap();
    ccs_limited.read_tls(&mut &[20, 3, 3, 0, 1, 1][..]).unwrap();
    assert!(ccs_limited.process_new_packets().is_err());

    let mut server = RealityServerConnection::new(RealityServerConfig {
        private_key: secret,
        short_ids: vec![[0; 8]],
        ..Default::default()
    })
    .with_target_shape(shape);

    server.read_tls(&mut &request[..]).unwrap();
    server.process_new_packets().unwrap();
    let mut response = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut response).unwrap();
    }
    client.read_tls(&mut &response[..]).unwrap();
    client.process_new_packets().unwrap();
    let mut finished = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut finished).unwrap();
    }
    server.read_tls(&mut &finished[..]).unwrap();
    server.process_new_packets().unwrap();

    response.clear();
    while server.wants_write() {
        server.write_tls(&mut response).unwrap();
    }
    let first = 5 + u16::from_be_bytes([response[3], response[4]]) as usize;
    assert_eq!(first, 64);
    let second = 5 + u16::from_be_bytes([response[first + 3], response[first + 4]]) as usize;
    assert_eq!(second, 96);
    assert_eq!(response.len(), first + second);
    client.read_tls(&mut &response[..]).unwrap();
    client.process_new_packets().unwrap();
}

#[test]
fn client_finished_reassembles_across_encrypted_records() {
    use crate::reality::reality_client_connection::{RealityClientConfig, RealityClientConnection};
    let secret = [7; 32];
    let public = x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(secret));
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
    client.read_tls(&mut &wire[..]).unwrap();
    client.process_new_packets().unwrap();
    wire.clear();
    while client.wants_write() {
        client.write_tls(&mut wire).unwrap();
    }
    let HandshakeState::AwaitingClientFinished {
        client_handshake_traffic_secret,
        cipher_suite,
        ..
    } = &server.handshake_state
    else {
        panic!("missing handshake keys");
    };
    let suite = *cipher_suite;
    let (key, iv) = derive_traffic_keys(client_handshake_traffic_secret, suite).unwrap();
    let size = u16::from_be_bytes([wire[3], wire[4]]);
    let plaintext = ztls::aead::decrypt_handshake_message(
        suite,
        &key,
        &iv,
        0,
        &wire[5..5 + size as usize],
        size,
    )
    .unwrap();
    let key = AeadKey::new(suite, &key).unwrap();
    let mut seq = 0;
    for chunk in plaintext.chunks(2) {
        let mut wire = Vec::new();
        RecordEncryptor::new(&key, &iv, &mut seq)
            .encrypt_handshake(chunk, &mut wire)
            .unwrap();
        server.read_tls(&mut &wire[..]).unwrap();
        server.process_new_packets().unwrap();
    }
    assert!(!server.is_handshaking());
}
