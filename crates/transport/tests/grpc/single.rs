use super::*;
#[tokio::test]
async fn single_client_keeps_original_framing_and_reverse_half_close_without_spawning_workers() {
    timeout(Duration::from_secs(5), async {
        for multi_mode in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let profile = OwnedGrpcProfile {
                service_names: vec!["single/Tun".into()],
                multi_mode,
                ..Default::default()
            };
            let server_profile = profile.clone();
            let (done, finished) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let incoming = accept_grpc_connection(io, &server_profile).unwrap();
                let mut io = incoming.accept().await.unwrap();
                let mut bytes = Vec::new();
                io.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, vec![0x42; 196_613]);
                AsyncWriteExt::write_all(&mut io, &bytes).await.unwrap();
                AsyncWriteExt::shutdown(&mut io).await.unwrap();
                finished.await.unwrap();
                incoming.close();
            });
            let io = tokio::net::TcpStream::connect(address).await.unwrap();
            let (mut io, driver) = PreparedSingleClient::prepare(io, &profile, "localhost", None)
                .await
                .unwrap()
                .into_parts();
            // No application byte progresses unless this returned real driver
            // is polled by the caller. This test makes no native-close claim.
            let app = async {
                AsyncWriteExt::write_all(&mut io, &vec![0x42; 196_613])
                    .await
                    .unwrap();
                AsyncWriteExt::shutdown(&mut io).await.unwrap();
                let mut bytes = Vec::new();
                io.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, vec![0x42; 196_613]);
                done.send(()).unwrap();
            };
            let (_, _) = tokio::join!(app, driver);
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
#[test]
fn single_client_cold_validation_checks_the_full_service_set() {
    let mut profile = OwnedGrpcProfile {
        service_names: vec!["first".into(), "second".into()],
        ..Default::default()
    };
    validate_single_client_profile(&profile, "localhost").unwrap();
    profile.authority = Some("invalid authority with spaces".into());
    assert!(validate_single_client_profile(&profile, "localhost").is_err());
    profile.service_names.clear();
    assert!(validate_single_client_profile(&profile, "localhost").is_err());
}
