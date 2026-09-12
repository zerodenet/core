use super::*;

#[tokio::test]
#[ignore = "requires fixed shadowsocks-rust 1.21.2"]
async fn official_server_accepts_continuous_session_and_large_response() {
    require_binary("ssserver");
    for (cipher, method, identity, user) in methods() {
        let address = available_dual_address().await;
        let echo = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        let echo_task = tokio::spawn(async move {
            let mut buf = vec![0; 65535];
            for _ in 0..1100 {
                let (n, peer) = echo.recv_from(&mut buf).await.unwrap();
                echo.send_to(&buf[..n], peer).await.unwrap();
            }
        });
        let (_server, _config) = spawn_ssserver(address, method, identity, user, "tcp_and_udp");
        drop(connect_retry(address).await);
        let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let codec = ShadowsocksDatagramCodec::new(cipher, format!("{identity}:{user}"));
        let mut response = vec![0; 65535];
        for sequence in 0..1100u32 {
            let payload = if sequence == 1099 {
                vec![7; 8000]
            } else {
                sequence.to_be_bytes().to_vec()
            };
            let request = codec
                .encode(&Address::Ipv4([127, 0, 0, 1]), echo_addr.port(), &payload)
                .unwrap();
            client.send_to(&request, address).await.unwrap();
            let (n, _) =
                tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut response))
                    .await
                    .expect("official response")
                    .unwrap();
            assert_eq!(codec.decode(&response[..n]).unwrap().2, payload);
            assert!(codec.decode(&response[..n]).is_none());
        }
        echo_task.await.unwrap();
    }
}
