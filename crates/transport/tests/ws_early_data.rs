#![cfg(feature = "ws")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Profile {
    path: &'static str,
}
impl zero_traits::WebSocketTransportProfile for Profile {
    fn path(&self) -> &str {
        self.path
    }
    fn header_pairs(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}
#[tokio::test]
async fn websocket_early_data_and_oversized_first_write_preserve_order() {
    for size in [12, 2048, 4096] {
        let (a, b) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move {
            let mut stream = zero_transport::ws::accept_ws(b, "/ws?ed=2048")
                .await
                .unwrap();
            let mut bytes = vec![0; size + 4];
            stream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], vec![128; size]);
            assert_eq!(&bytes[size..], b"tail");
            stream.write_all(&bytes).await.unwrap();
            stream.flush().await.unwrap();
        });
        let mut stream = zero_transport::ws::connect_ws(
            a,
            &Profile {
                path: "/ws?ed=2048",
            },
            "localhost",
            80,
        )
        .await
        .unwrap();
        stream.write_all(&vec![128; size]).await.unwrap();
        stream.write_all(b"tail").await.unwrap();
        stream.flush().await.unwrap();
        let mut bytes = vec![0; size + 4];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], vec![128; size]);
        assert_eq!(&bytes[size..], b"tail");
        task.await.unwrap();
    }
}
