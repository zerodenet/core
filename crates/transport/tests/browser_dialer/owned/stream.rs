//! Hosted data actually traverses the paid socket, then its original returns.
use super::*;
use fixture::{deadline, drive, Fixture};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;
#[tokio::test]
async fn actual_hosted_wire_bytes_and_held_stream_keep_original_and_backing_until_real_return() {
    timeout(Duration::from_secs(5), async {
        let mut actual = Fixture::start().await;
        let mut peer = drive(&mut actual.scope, fixture::browser(&actual.source)).await;
        let dialer = actual.source.dialer();
        let (stream, ()) = drive(&mut actual.scope, async {
            tokio::join!(
                dialer.dial_websocket("wss://relay.example/ws", Some(b"early"), 0),
                async {
                    let task = peer.next().await.unwrap().unwrap().into_text().unwrap();
                    let task: serde_json::Value = serde_json::from_str(&task).unwrap();
                    assert_eq!(task["method"], "WS");
                    assert_eq!(task["url"], "wss://relay.example/ws");
                    assert_eq!(task["extra"]["protocol"], "ZWFybHk");
                    peer.send(Message::Text("ok".into())).await.unwrap();
                }
            )
        })
        .await;
        let mut stream = stream.unwrap();
        let original = stream.original().unwrap().clone();
        let (id, witness) = original.original_task().unwrap();
        assert!(witness
            .scope_identity()
            .same_scope(&actual.scope.registrar().scope_identity()));
        let request = vec![0x35; 8192];
        let response = vec![0x79; 8192];
        drive(&mut actual.scope, async {
            tokio::join!(
                async {
                    stream.write_all(&request).await.unwrap();
                    stream.flush().await.unwrap();
                    let mut reply = vec![0; response.len()];
                    stream.read_exact(&mut reply).await.unwrap();
                    assert_eq!(reply, response);
                },
                async {
                    assert_eq!(peer.next().await.unwrap().unwrap().into_data(), request);
                    peer.send(Message::Binary(response.clone())).await.unwrap();
                }
            );
        })
        .await;
        let book = actual.provider.totals.buffer_budget();
        assert!(
            book.used_bytes() > 0,
            "the actual socket and codec backing is still live"
        );
        actual.source.revoke(CloseReason::Shutdown);
        actual.scope.begin_shutdown();
        let mut matching_join = witness
            .try_joined()
            .is_some_and(|joined| joined.registration_released());
        while let Some(receipt) = actual.scope.next_receipt().await {
            receipt.result().unwrap();
            if receipt.id() == id {
                assert!(witness.matches_joined(receipt.joined()));
                matching_join = true;
            }
            actual.scope.acknowledge(&receipt).unwrap();
        }
        assert!(matching_join && actual.scope.is_drained());
        assert!(!actual
            .source
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(
            !original.is_returned(),
            "Scope joins/ACK cannot return the held native byte view"
        );
        assert!(
            book.used_bytes() > 0,
            "stop and ACK cannot refund held backing"
        );
        drop(stream);
        drop(peer);
        actual.finish().await;
        assert!(original.is_returned());
        assert_eq!(
            book.used_bytes(),
            0,
            "the actual final backing has now returned"
        );
    })
    .await
    .unwrap();
}
