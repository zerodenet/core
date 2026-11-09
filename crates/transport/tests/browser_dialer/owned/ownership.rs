use super::*;
use fixture::{deadline, drive, Fixture};
#[tokio::test]
async fn actual_pending_header_cancel_keeps_the_original_until_host_ack_and_native_close() {
    timeout(Duration::from_secs(5), async {
        let mut actual = Fixture::start().await;
        let _peer = tokio::net::TcpStream::connect(actual.source.local_addr().unwrap())
            .await
            .unwrap();
        let original = loop {
            let changed = actual.source.peers.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(original) = actual.source.peers.pending_header_for_test() {
                break original;
            }
            changed.await;
        };
        let (id, witness) = original.original_task().unwrap();
        assert!(witness
            .scope_identity()
            .same_scope(&actual.scope.registrar().scope_identity()));
        assert!(
            witness.try_joined().is_none(),
            "actual native read was observed Pending before cancellation"
        );
        actual.source.revoke(CloseReason::Shutdown);
        let receipt = timeout(Duration::from_secs(1), async {
            loop {
                let receipt = actual.scope.next_receipt().await.unwrap();
                receipt.result().unwrap();
                if receipt.id() == id {
                    break receipt;
                }
                actual.scope.acknowledge(&receipt).unwrap();
            }
        })
        .await
        .unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert!(
            !original.is_returned(),
            "body exit and view return cannot substitute for actual host ACK/native close"
        );
        assert!(!actual
            .source
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(
            !original.is_returned(),
            "shutdown still retains the exact unacknowledged original"
        );
        actual.scope.acknowledge(&receipt).unwrap();
        actual.finish().await;
        assert!(original.is_returned());
        assert!(original
            .0
            .closed
            .get()
            .unwrap()
            .matches(&original.0.original));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn unobserved_idle_custody_retires_the_actual_socket_after_its_original_scope_receipt() {
    timeout(Duration::from_secs(5), async {
        let mut actual = Fixture::start().await;
        let mut peer = drive(&mut actual.scope, fixture::browser(&actual.source)).await;
        let original = loop {
            if let Some(original) = actual.source.dialer().inner.idle.original_for_test() {
                break original;
            }
            tokio::task::yield_now().await;
        };
        let (id, witness) = original.original_task().unwrap();
        let receipt = loop {
            let receipt = actual.scope.next_receipt().await.unwrap();
            receipt.result().unwrap();
            if receipt.id() == id {
                break receipt;
            }
            actual.scope.acknowledge(&receipt).unwrap();
        };
        assert!(witness.matches_joined(receipt.joined()));
        actual.source.revoke(CloseReason::Shutdown); // no business caller/observer ever received this socket
        assert!(!original.is_returned());
        assert!(!receipt.joined().registration_released());
        assert!(!actual
            .source
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(
            !original.is_returned(),
            "shutdown still retains the exact unacknowledged original"
        );
        actual.scope.acknowledge(&receipt).unwrap();
        actual.finish().await;
        assert!(original.is_returned());
        use futures_util::StreamExt;
        let last = peer.next().await;
        assert!(
            last.is_none()
                || matches!(
                    last,
                    Some(Err(_)) | Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))
                )
        );
    })
    .await
    .unwrap();
}
