#![cfg(all(feature = "runtime", feature = "tokio"))]
use base64::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vless::encryption::{config::EncryptionConfig, EncryptionClient, EncryptionServer};

fn base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}
fn pair(mask: &str, seeds: &[Vec<u8>]) -> (EncryptionClient, EncryptionServer) {
    let keys = seeds
        .iter()
        .map(|key| base64(key))
        .collect::<Vec<_>>()
        .join(".");
    let server = EncryptionServer::new(
        EncryptionConfig::server(&format!("mlkem768x25519plus.{mask}.600s.100-35-35.{keys}"))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let keys = server
        .public_keys()
        .iter()
        .map(|key| base64(key))
        .collect::<Vec<_>>()
        .join(".");
    let client = EncryptionClient::new(
        EncryptionConfig::client(&format!("mlkem768x25519plus.{mask}.0rtt.100-35-35.{keys}"))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    (client, server)
}

#[tokio::test]
async fn encryption_all_masks_key_types_chains_and_resumption() {
    for mask in ["native", "xorpub", "random"] {
        for seeds in [
            vec![vec![1; 32]],
            vec![vec![2; 64]],
            vec![vec![3; 32], vec![4; 64], vec![5; 32]],
        ] {
            let (client, server) = pair(mask, &seeds);
            for round in 0..3 {
                let (a, b) = tokio::io::duplex(4096);
                let payload: Vec<u8> = (0..40000).map(|index| (index + round) as u8).collect();
                let expected = payload.clone();
                let server = server.clone();
                let serving = tokio::spawn(async move {
                    let mut connection = server.handshake(b).await.unwrap();
                    let mut bytes = vec![0; expected.len()];
                    connection.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(bytes, expected);
                    connection.write_all(&bytes).await.unwrap();
                    connection.shutdown().await.unwrap();
                });
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let mut connection = client.handshake(a).await.unwrap();
                    connection.write_all(&payload).await.unwrap();
                    connection.flush().await.unwrap();
                    let mut response = Vec::new();
                    connection.read_to_end(&mut response).await.unwrap();
                    assert_eq!(response, payload);
                    serving.await.unwrap();
                })
                .await
                .unwrap();
            }
        }
    }
}

#[test]
fn encryption_rejects_invalid_configuration_and_noncanonical_kem_keys() {
    for config in [
        "",
        "aes-128-gcm",
        "mlkem768x25519plus.native.1rtt",
        "mlkem768x25519plus.native.1rtt.100-34-35",
        "mlkem768x25519plus.native.1rtt.100-35-65554",
    ] {
        assert!(EncryptionConfig::client(config).is_err(), "{config}");
    }
    let key = base64(&[255; 1184]);
    let config = EncryptionConfig::client(&format!("mlkem768x25519plus.native.1rtt.{key}"))
        .unwrap()
        .unwrap();
    assert!(EncryptionClient::new(config).is_err());
}

#[tokio::test]
async fn vision_bypass_preserves_fragmented_random_headers() {
    for mask in ["native", "xorpub", "random"] {
        let (client, server) = pair(mask, &[vec![7; 32]]);
        let (a, b) = tokio::io::duplex(1024);
        let (client, server) = tokio::join!(client.handshake(a), server.handshake(b));
        let mut client = client.unwrap();
        let mut server = server.unwrap();
        let client_control = zero_traits::AsyncSocket::transport_bypass_control(&client).unwrap();
        let server_control = zero_traits::AsyncSocket::transport_bypass_control(&server).unwrap();
        client.write_all(b"switch").await.unwrap();
        client.flush().await.unwrap();
        let mut prefix = [0; 6];
        server.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, b"switch");
        client_control.request_write_bypass();
        server_control.request_read_bypass();
        let record = [&[23, 3, 3, 0, 33][..], &[8; 33][..]].concat();
        for byte in &record {
            client.write_all(&[*byte]).await.unwrap();
            client.flush().await.unwrap();
        }
        let mut read = vec![0; record.len()];
        server.read_exact(&mut read).await.unwrap();
        assert_eq!(read, record);
        server.write_all(b"reply").await.unwrap();
        server.flush().await.unwrap();
        let mut prefix = [0; 5];
        client.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, b"reply");
        server_control.request_write_bypass();
        client_control.request_read_bypass();
        server.write_all(&record).await.unwrap();
        server.flush().await.unwrap();
        client.read_exact(&mut read).await.unwrap();
        assert_eq!(read, record);
    }
}
