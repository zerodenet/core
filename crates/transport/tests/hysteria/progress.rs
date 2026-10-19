use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn negotiated_small_bandwidth_keeps_response_and_fin_sendable_below_one_packet_bdp() {
    timeout(Duration::from_secs(10), async {
        // A small bandwidth-delay product deterministically reaches the
        // minimum window, including on a loaded loopback runner.
        let profile = Profile::from_options(OptionsRef {
            auth: "secret",
            uplink_bytes_per_sec: 1_000,
            downlink_bytes_per_sec: 1_000,
            ..Default::default()
        })
        .unwrap();
        let (endpoint, server_profile) = endpoint_with_profile(profile.clone());
        let address = endpoint.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let incoming = accept_connection(connection.clone(), &server_profile);
            let mut stream = incoming.accept().await.unwrap();
            let mut ping = [0; 4];
            stream.read_exact(&mut ping).await.unwrap();
            assert_eq!(&ping, b"ping");
            println!(
                "server window={} mtu={} rtt={:?}",
                connection.congestion_state().window(),
                connection.stats().path.current_mtu,
                connection.rtt()
            );
            stream.write_all(b"pong").await.unwrap();
            stream.shutdown().await.unwrap();
            connection.closed().await;
        });
        let connection = dial(address, &profile).await;
        let client = Client::authenticate(connection, &profile).await.unwrap();
        let mut stream = client.open().await.unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, b"pong");
        drop(stream);
        drop(client);
        server.await.unwrap();
    })
    .await
    .expect("small BDP must not strand response or FIN within the original 10s deadline");
}
