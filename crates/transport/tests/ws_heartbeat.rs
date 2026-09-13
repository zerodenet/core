#![cfg(feature = "ws")]
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncReadExt;
use tokio_tungstenite::tungstenite::Message;
struct Profile;
impl zero_traits::WebSocketTransportProfile for Profile {
    fn path(&self) -> &str {
        "/"
    }
    fn header_pairs(&self) -> Vec<(String, String)> {
        vec![]
    }
    fn heartbeat_period_secs(&self) -> u32 {
        1
    }
}
#[tokio::test(start_paused = true)]
async fn idle_websocket_sends_empty_pings_without_exposing_control_frames_as_data() {
    let (a, b) = tokio::io::duplex(8192);
    let peer = tokio::spawn(async move {
        let mut stream = tokio_tungstenite::accept_async(b).await.unwrap();
        for _ in 0..2 {
            assert_eq!(stream.next().await.unwrap().unwrap(), Message::Ping(vec![]));
            stream.flush().await.unwrap();
        }
        stream
            .send(Message::Binary(b"alive".to_vec()))
            .await
            .unwrap();
    });
    let mut stream = zero_transport::ws::connect_ws(a, &Profile, "localhost", 80)
        .await
        .unwrap();
    let mut bytes = [0; 5];
    // Cancelling an idle read must not restart the heartbeat deadline.
    let started = tokio::time::Instant::now();
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(500),
        stream.read_exact(&mut bytes)
    )
    .await
    .is_err());
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.read_exact(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&bytes, b"alive");
    assert_eq!(started.elapsed(), std::time::Duration::from_secs(2));
    peer.await.unwrap();
}
