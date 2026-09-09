use super::*;

pub(super) async fn mixed_traffic(socks_port: u16) {
    let mut tasks = tokio::task::JoinSet::new();
    for marker in 1..=8u8 {
        let udp = marker % 2 == 0;
        let port = if udp { free_udp_port() } else { free_port() };
        let payload = vec![marker; 1600];
        let echo = if udp {
            spawn_udp_echo(port, payload.len()).await
        } else {
            spawn_tcp_echo(port, payload.len()).await
        };
        tasks.spawn(async move {
            let received = if udp {
                socks5_udp_echo(socks_port, port, &payload).await
            } else {
                socks5_tcp_echo(socks_port, port, &payload).await
            };
            assert_eq!(received, payload);
            echo.await.unwrap();
        });
    }
    timeout(Duration::from_secs(15), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn concurrent_tcp_and_fragmented_udp_authenticate_once_with_official_server() {
    interop_case(true, false, false, true).await;
}
