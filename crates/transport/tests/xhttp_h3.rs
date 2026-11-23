#![cfg(all(feature = "split_http", feature = "quic"))]
use std::{future::Future, sync::Arc, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_transport::{
    profile::OwnedSplitHttpProfile,
    split_http::{accept_xhttp_h3_connection, connect_xhttp_h3, SplitHttpRegistry},
};
fn endpoints() -> (quinn::Endpoint, quinn::Endpoint) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    let server = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(
        zero_transport::quic::client_config(true, None, &[b"h3".to_vec()], None).unwrap(),
    );
    (server, client)
}

#[tokio::test]
async fn http3_runs_all_xhttp_modes_with_bounded_bidirectional_io() {
    for mode in ["packet-up", "stream-up", "stream-one"] {
        let (server, client) = endpoints();
        let mut profile = OwnedSplitHttpProfile {
            host: Some("localhost".into()),
            path: "/h3?test=1".into(),
            mode: mode.into(),
            options: Default::default(),
        };
        profile.options.session_placement = "header".into();
        profile.options.seq_placement = "cookie".into();
        profile.options.x_padding_obfs_mode = true;
        profile.options.x_padding_placement = "query".into();
        profile.options.x_padding_method = "tokenish".into();
        let server_profile = profile.clone();
        let remote = server.local_addr().unwrap();
        let (done, finished) = tokio::sync::oneshot::channel();
        let echo = tokio::spawn(async move {
            let connection = server.accept().await.unwrap().await.unwrap();
            let incoming =
                accept_xhttp_h3_connection(connection, &server_profile, &SplitHttpRegistry::new());
            let stream = incoming.accept().await.unwrap();
            let (mut reader, mut writer) = tokio::io::split(stream);
            let mut remaining = 300_123;
            let mut buffer = [0; 4096];
            while remaining > 0 {
                let n = reader
                    .read(&mut buffer[..remaining.min(4096)])
                    .await
                    .unwrap();
                assert!(n > 0);
                writer.write_all(&buffer[..n]).await.unwrap();
                remaining -= n;
            }
            // Keep the connection owner until the peer receives its final body.
            let _ = finished.await;
        });
        let connection = client.connect(remote, "localhost").unwrap().await.unwrap();
        let stream = connect_xhttp_h3(connection, &profile).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(stream);
        let data: Vec<u8> = (0..300_123).map(|i| (i % 251) as u8).collect();
        let sent = data.clone();
        let upload = tokio::spawn(async move {
            writer.write_all(&sent).await.unwrap();
            writer.flush().await.unwrap();
            writer
        });
        let mut received = vec![0; data.len()];
        tokio::time::timeout(Duration::from_secs(15), reader.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, data, "mode={mode}");
        drop(upload.await.unwrap());
        drop(reader);
        let _ = done.send(());
        echo.await.unwrap();
    }
}

#[tokio::test]
async fn http3_peer_stop_retires_the_idle_download_on_a_live_connection() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (server, client) = endpoints();
        let remote = server.local_addr().unwrap();
        let (client, server) = tokio::join!(client.connect(remote, "localhost").unwrap(), async {
            server.accept().await.unwrap().await
        });
        let client = client.unwrap();
        let server = server.unwrap();
        let profile = OwnedSplitHttpProfile {
            host: Some("localhost".into()),
            path: "/h3".into(),
            mode: "packet-up".into(),
            options: Default::default(),
        };
        let source = accept_xhttp_h3_connection(server, &profile, &SplitHttpRegistry::new());
        let (mut connection, mut sender) =
            h3::client::new(h3_quinn::Connection::new(client.clone()))
                .await
                .unwrap();
        let driver = tokio::spawn(async move { connection.wait_idle().await });
        let request = http::Request::get("https://localhost/h3/idle")
            .header(
                "referer",
                format!("https://localhost/h3?x_padding={}", "X".repeat(128)),
            )
            .body(())
            .unwrap();
        let mut request = sender.send_request(request).await.unwrap();
        request.finish().await.unwrap();
        assert_eq!(request.recv_response().await.unwrap().status(), 200);
        let mut stream = source.accept().await.unwrap();
        let mut byte = [0];
        let mut read = std::pin::pin!(stream.read(&mut byte));
        std::future::poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        request.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
        assert_eq!(read.await.unwrap(), 0);
        assert!(client.close_reason().is_none());
        client.close(0u32.into(), b"test complete");
        driver.await.unwrap();
    })
    .await
    .expect("the peer stop retires the pending logical download");
}
