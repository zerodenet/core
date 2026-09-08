use super::test_fixtures::pair_with_settings;
use crate::settings::Settings;
use tokio::time::{timeout, Duration, Instant};

#[tokio::test]
async fn quic_streams_and_datagrams_share_one_connection_ceiling_without_principal() {
    timeout(Duration::from_secs(15), async {
        let settings = Settings {
            upload: 65_536,
            ..Default::default()
        };
        let (client, server) = pair_with_settings(settings, Default::default()).await;
        // No authentication identity or principal registry is involved here.
        assert!(client.congestion_state().pacing_rate().unwrap() <= settings.upload);
        let start = Instant::now();
        let send = async {
            let first = async {
                let (mut tx, _rx) = client.open_bi().await.unwrap();
                tx.write_all(&vec![1; 65_536]).await.unwrap();
                tx.finish().unwrap();
            };
            let second = async {
                let (mut tx, _rx) = client.open_bi().await.unwrap();
                tx.write_all(&vec![2; 65_536]).await.unwrap();
                tx.finish().unwrap();
            };
            tokio::join!(first, second);
            for _ in 0..16 {
                client
                    .send_datagram(bytes::Bytes::from(vec![3; 1000]))
                    .unwrap();
            }
        };
        let receive = async {
            let streams = async {
                let (_tx, mut first) = server.accept_bi().await.unwrap();
                let (_tx, mut second) = server.accept_bi().await.unwrap();
                let (first, second) =
                    tokio::join!(first.read_to_end(65_536), second.read_to_end(65_536));
                assert_eq!(first.unwrap().len() + second.unwrap().len(), 131_072);
            };
            let datagrams = async {
                for _ in 0..16 {
                    assert_eq!(server.read_datagram().await.unwrap().len(), 1000);
                }
            };
            tokio::join!(streams, datagrams);
        };
        tokio::join!(send, receive);
        // Wide margin for packet bursts and handshake timing; independent
        // per-stream caps would finish these transfers in roughly one second.
        assert!(
            start.elapsed() >= Duration::from_millis(1600),
            "connection budget multiplied across streams: {:?}",
            start.elapsed()
        );
        client.close(0u32.into(), b"done");
    })
    .await
    .unwrap();
}
