use super::*;
use crate::reality::reality_server_connection::{RealityServerConfig, RealityServerConnection};

fn pair(suite: CipherSuite) -> (RealityClientConnection, RealityServerConnection) {
    let private = [7; 32];
    let mut client = RealityClientConnection::new(RealityClientConfig {
        public_key: *PublicKey::from(&StaticSecret::from(private)).as_bytes(),
        server_name: "localhost".into(),
        ..Default::default()
    })
    .unwrap();
    let mut server = RealityServerConnection::new(RealityServerConfig {
        private_key: private,
        short_ids: vec![[0; 8]],
        cipher_suites: vec![suite],
        ..Default::default()
    });
    send_client(&mut client, &mut server);
    send_server(&mut server, &mut client);
    send_client(&mut client, &mut server);
    assert!(!client.is_handshaking() && !server.is_handshaking());
    (client, server)
}
fn send_client(client: &mut RealityClientConnection, server: &mut RealityServerConnection) {
    let mut wire = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut wire).unwrap();
    }
    for chunk in wire.chunks(4096) {
        server.read_tls(&mut &chunk[..]).unwrap();
        server.process_new_packets().unwrap();
    }
}
fn send_server(server: &mut RealityServerConnection, client: &mut RealityClientConnection) {
    let mut wire = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut wire).unwrap();
    }
    for chunk in wire.chunks(4096) {
        client.read_tls(&mut &chunk[..]).unwrap();
        client.process_new_packets().unwrap();
    }
}
#[test]
fn key_updates_preserve_bidirectional_application_records_for_all_suites() {
    for suite in ztls::cipher::DEFAULT_CIPHER_SUITES {
        let (mut client, mut server) = pair(*suite);
        for requested in [false, true, true, false] {
            client.update_write_key(requested).unwrap();
            client.writer().write_all(b"client after update").unwrap();
            send_client(&mut client, &mut server);
            let mut read = [0; 19];
            server.reader().read_exact(&mut read).unwrap();
            assert_eq!(&read, b"client after update");
            server.writer().write_all(b"server after update").unwrap();
            send_server(&mut server, &mut client);
            client.reader().read_exact(&mut read).unwrap();
            assert_eq!(&read, b"server after update");
            server.update_write_key(requested).unwrap();
            send_server(&mut server, &mut client);
            send_client(&mut client, &mut server);
        }
    }
}
#[test]
fn key_update_rejects_old_epoch_records_and_bad_outer_headers() {
    for bad_header in [false, true] {
        let (mut client, mut server) = pair(CipherSuite::AES_128_GCM_SHA256);
        server.writer().write_all(b"old epoch").unwrap();
        let mut old = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut old).unwrap();
        }
        if bad_header {
            old[1] = 2;
        } else {
            // Accept the old record once, then rotate and attempt to replay it.
            client.read_tls(&mut &old[..]).unwrap();
            client.process_new_packets().unwrap();
            server.update_write_key(false).unwrap();
            send_server(&mut server, &mut client);
        }
        client.read_tls(&mut &old[..]).unwrap();
        assert!(client.process_new_packets().is_err());
    }
}

#[test]
fn every_versioned_fingerprint_authenticates_and_relays_reality_records() {
    for &profile in ClientHelloProfile::VERSIONED {
        let private = [7; 32];
        let mut client = RealityClientConnection::new(RealityClientConfig {
            public_key: *PublicKey::from(&StaticSecret::from(private)).as_bytes(),
            server_name: "localhost".into(),
            client_hello_profile: profile,
            ..Default::default()
        })
        .unwrap();
        let mut server = RealityServerConnection::new(RealityServerConfig {
            private_key: private,
            short_ids: vec![[0; 8]],
            ..Default::default()
        });
        send_client(&mut client, &mut server);
        send_server(&mut server, &mut client);
        send_client(&mut client, &mut server);
        assert!(client.is_authenticated_reality(), "{profile}");
        client.writer().write_all(b"fingerprint payload").unwrap();
        send_client(&mut client, &mut server);
        let mut payload = [0; 19];
        server.reader().read_exact(&mut payload).unwrap();
        assert_eq!(&payload, b"fingerprint payload");
        server.writer().write_all(&payload).unwrap();
        send_server(&mut server, &mut client);
        client.reader().read_exact(&mut payload).unwrap();
        assert_eq!(&payload, b"fingerprint payload");
    }
}
