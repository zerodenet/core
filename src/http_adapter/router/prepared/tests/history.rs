use super::*;

#[tokio::test]
async fn actual_history_snapshot_prepays_same_root_and_releases_history_before_pending_output() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
        time::Duration,
    };
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(3), async {
        let (handle, book) = fixture();
        let engine = handle.engine_handle().inner();
        engine.emit_warning("original-雪", "original immutable event payload");
        let expected =
            serde_json::to_value(ApiResponse::ok(engine.events_snapshot(&Default::default())))
                .unwrap();
        let initial = book.limits();
        let mut tiny = initial;
        tiny.messages.response_bytes_total = 1;
        book.prepare(tiny).unwrap().commit().unwrap();
        assert_eq!(
            prepare(&handle, Selected::Events, &book)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(book.usage().response_bytes, 0);
        // A refusal returned the actual history guard; no future event was
        // published or hidden just to make admission succeed.
        book.prepare(initial).unwrap().commit().unwrap();
        let body = prepare(&handle, Selected::Events, &book).unwrap();
        let original = body.bytes_for_test();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&original).unwrap(),
            expected
        );
        assert_eq!(book.usage().response_bytes, original.len());
        let (mut sending, mut receiving) = tokio::io::duplex(1);
        let mut write = Box::pin(crate::http_adapter::output::write_response(
            &mut sending,
            "HTTP/1.1 200 OK\r\n",
            &body,
            &book,
        ));
        poll_fn(|cx| {
            assert!(write.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(book.usage().response_bytes > original.len());
        let sequence = engine.latest_event_sequence();
        engine.emit_warning(
            "later",
            "actual history can publish while the original output is pending",
        );
        assert_eq!(engine.latest_event_sequence(), sequence + 1);
        assert_eq!(body.bytes_for_test(), original);
        let read = async {
            let mut wire = Vec::new();
            receiving.read_to_end(&mut wire).await.unwrap();
            wire
        };
        let (written, wire) = tokio::join!(write, read);
        written.unwrap();
        assert!(wire.ends_with(&original));
        assert_eq!(book.usage().response_bytes, original.len());
        drop(body);
        assert_eq!(book.usage().response_bytes, 0);
        // Duplex EOF is only write-half completion; no native-close or Scope
        // receipt is claimed by this byte/history regression.
    })
    .await
    .unwrap();
}
