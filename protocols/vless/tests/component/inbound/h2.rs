use super::*;
#[tokio::test]
async fn standard_h2_preserves_large_bidirectional_data_and_original_carrier_cleanup() {
    timeout(Duration::from_secs(10), async {
        let profile = OwnedH2Profile {
            host: None,
            path: "/vless".into(),
        };
        let request = request_profiles(USER, None, Some(&profile));
        let mut host = fixture::Host::with_request(request).await;
        let (socket, accepted) = host.accept().await;
        let mut client = zero_transport::h2::connect_h2(socket, &profile, "localhost", 443)
            .await
            .unwrap();
        client.write_all(&header(1)).await.unwrap();
        let (request, _) = accepted.await.unwrap();
        let AcceptedRequest::Stream(request) = request else {
            panic!("standard H2 stream")
        };
        let mut stream = request
            .respond(ReplyOutcome::LocalChannelReady)
            .await
            .unwrap()
            .unwrap();
        let mut response = [0; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, [0, 0]);
        let bytes = vec![73; 256 * 1024];
        let upload = async {
            client.write_all(&bytes).await.unwrap();
            client.flush().await.unwrap();
            let mut output = vec![0; bytes.len()];
            client.read_exact(&mut output).await.unwrap();
            assert_eq!(output, bytes);
        };
        let relay = async {
            let mut received = vec![0; bytes.len()];
            stream.read_exact(&mut received).await.unwrap();
            assert_eq!(received, bytes);
            stream.write_all(&received).await.unwrap();
            stream.flush().await.unwrap();
        };
        tokio::join!(upload, relay);
        // Keep the logical byte view live through cancellation: only the
        // original provider may retire the actual accepted TCP carrier.
        host.stop.cancel();
        drop(stream);
        drop(client);
        host.close(2).await;
    })
    .await
    .unwrap();
}
