//! Loopback test: Zero mieru outbound (client) <-> inbound (server) over an
//! in-memory pipe.
//!
//! The outbound side is validated end-to-end against upstream mita. This test
//! therefore verifies the inbound mirrors the server side of the mieru
//! handshake correctly — without needing any external binary.

#![cfg(feature = "crypto")]

use mieru::{
    inbound::{MieruInbound, MieruInboundProfile},
    MieruOutbound,
};
use zero_traits::AsyncSocket;

/// Adapter implementing `AsyncSocket` over a tokio duplex half.
struct DuplexSock(tokio::io::DuplexStream);

impl AsyncSocket for DuplexSock {
    type Error = std::io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        use tokio::io::AsyncReadExt;
        self.0.read(buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        use tokio::io::AsyncWriteExt;
        self.0.write_all(buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        use tokio::io::AsyncWriteExt;
        self.0.shutdown().await
    }
}

#[tokio::test]
async fn mieru_outbound_inbound_handshake_loopback() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let mut client = DuplexSock(client_io);
    let mut server = DuplexSock(server_io);
    let users: Vec<(String, String)> = vec![(
        "zero_test_user".to_string(),
        "change_this_password_2026".to_string(),
    )];

    let client_handle = tokio::spawn(async move {
        MieruOutbound::connect(&mut client, "zero_test_user", "change_this_password_2026").await
    });
    let inbound = MieruInbound;
    let server_handle =
        tokio::spawn(async move { inbound.accept_request(&mut server, &users).await });

    let _outbound = client_handle
        .await
        .expect("client task join")
        .expect("outbound connect should succeed");
    let _accept = server_handle
        .await
        .expect("server task join")
        .expect("inbound accept should succeed");
    // Both sides completed the mieru handshake: openSessionRequest ->
    // openSessionResponse, with matching key derivation and nonce handling.
}

#[tokio::test]
async fn mieru_inbound_attributes_the_matched_user() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let mut client = DuplexSock(client_io);
    let mut server = DuplexSock(server_io);
    let profile = MieruInboundProfile::from_config_users([
        ("first-user", "first-secret", Some("subscription:first")),
        ("second-user", "second-secret", Some("subscription:second")),
    ]);

    let client_handle = tokio::spawn(async move {
        MieruOutbound::connect(&mut client, "second-user", "second-secret").await
    });
    let server_handle = tokio::spawn(async move { profile.accept_request(&mut server).await });

    let _outbound = client_handle
        .await
        .expect("client task join")
        .expect("outbound connect should succeed");
    let accept = server_handle
        .await
        .expect("server task join")
        .expect("inbound accept should succeed");

    assert_eq!(
        accept.auth().principal_key.as_deref(),
        Some("subscription:second")
    );
}

#[tokio::test]
async fn mieru_inbound_uses_the_matched_username_as_default_principal() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let mut client = DuplexSock(client_io);
    let mut server = DuplexSock(server_io);
    let users = vec![("subscriber-7".to_owned(), "secret-7".to_owned())];

    let client_handle = tokio::spawn(async move {
        MieruOutbound::connect(&mut client, "subscriber-7", "secret-7").await
    });
    let server_handle =
        tokio::spawn(async move { MieruInbound.accept_request(&mut server, &users).await });

    let _outbound = client_handle
        .await
        .expect("client task join")
        .expect("outbound connect should succeed");
    let accept = server_handle
        .await
        .expect("server task join")
        .expect("inbound accept should succeed");

    assert_eq!(accept.auth().principal_key.as_deref(), Some("subscriber-7"));
}

#[tokio::test]
async fn mieru_inbound_rejects_modified_credentials() {
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let mut client = DuplexSock(client_io);
    let mut server = DuplexSock(server_io);
    let users = vec![("subscriber-7".to_owned(), "secret-7".to_owned())];

    let client_handle = tokio::spawn(async move {
        MieruOutbound::connect(&mut client, "subscriber-7", "modified-secret").await
    });
    let server_handle =
        tokio::spawn(async move { MieruInbound.accept_request(&mut server, &users).await });

    let server_result = server_handle.await.expect("server task join");
    assert!(server_result.is_err(), "modified credentials were accepted");

    let client_result = client_handle.await.expect("client task join");
    assert!(
        client_result.is_err(),
        "client unexpectedly completed a rejected handshake"
    );
}
