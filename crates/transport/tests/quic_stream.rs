#![cfg(feature = "quic")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::ClientStream;
use zero_transport::quic::{connect_quic, QuicInbound};

#[tokio::test]
async fn raw_stream_retains_local_metadata_and_fin_before_last_client_handle_drops() {
    let root = std::env::temp_dir().join(format!("zero-quic-stream-{}", rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(root.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(root.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
    let listener = QuicInbound::bind(
        "127.0.0.1:0",
        "cert.pem",
        "key.pem",
        Some(&root),
        &[b"test-quic".to_vec()],
    )
    .await
    .unwrap();
    let local = listener.local_addr().unwrap();
    let (release, delayed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = listener.accept().await.unwrap();
        assert_eq!(stream.local_addr().unwrap(), local);
        assert_eq!(
            stream.tls_metadata(),
            (Some("localhost".into()), Some("test-quic".into()))
        );
        delayed.await.unwrap();
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .expect("all bytes and FIN remain readable after client drop");
        assert_eq!(bytes, vec![0x67; 32769]);
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut client = connect_quic(
            "127.0.0.1",
            local.port(),
            "localhost",
            true,
            &[b"test-quic".to_vec()],
            &zero_transport::OutboundDatagramSocketFactory::new(Default::default()),
        )
        .await
        .unwrap();
        client.write_all(&vec![0x67; 32769]).await.unwrap();
        client.shutdown().await.unwrap();
        drop(client);
        release.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
