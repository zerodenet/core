use super::*;
#[cfg(unix)]
#[tokio::test]
async fn actual_paid_decode_fault_handoff_unpins_retired_locals_but_keeps_external_payload_until_drop(
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let _serial = super::super::tests::unix::SERIAL.lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("decode.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = BufReader::new(stream);
        let mut request = String::new();
        peer.read_line(&mut request).await.unwrap();
        // The actual malformed original frame produces a typed DecodeFailure,
        // not a counter-only stand-in. Escalate that real fault in the same
        // registered IO task to exercise Scope/driver diagnostic retention.
        peer.get_mut().write_all(b"{invalid-json\n").await.unwrap();
        peer.get_mut().flush().await.unwrap();
    });
    let caller = Caller::begin(Limits::default()).await.unwrap();
    let original = caller.original.clone();
    let book = original.book.clone();
    original.fatal_decode.store(true, Ordering::Release);
    let result = caller
        .request(
            path.to_str().unwrap(),
            &crate::ipc::protocol::IpcRequest::Ping { id: None },
        )
        .await;
    assert!(result.is_err());
    drop(result);
    let error = caller
        .finish()
        .await
        .expect_err("real Scope decode fault must be delivered");
    server.await.unwrap();
    assert!(original.retired.load(Ordering::Acquire));
    assert_eq!(book.usage().connections, 0);
    let received = error.get_ref().unwrap().downcast_ref::<Handoff>().unwrap();
    let rows = received.retained_tasks().unwrap();
    assert_eq!(rows.joined.len(), 1);
    assert!(rows.joined[0].registration_released());
    assert!(lock(&original.faults.joined).is_empty());
    assert!(lock(&original.faults.native).is_none());
    assert!(lock(&original.faults.abandoned).is_none());
    let observed_error = original
        .driver
        .join()
        .await
        .expect_err("the original driver failed; handoff must not turn it into success");
    let observed = observed_error
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .expect("live receiver must preserve same original driver Arc");
    let same = received
        .driver
        .as_ref()
        .unwrap()
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(observed, same));
    assert!(
        book.usage().response_bytes > 0,
        "the external exact original DecodeFailure still owns its actual frame"
    );
    let mut target = book.limits();
    target.messages.response_bytes_total = 1;
    target.messages.request_bytes_total = 1;
    target.messages.ipc_frame_bytes = 1;
    target.messages.response_bytes = 1;
    assert_eq!(
        book.prepare(target).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(observed_error);
    drop(error);
    assert_eq!(
        book.usage().response_bytes,
        0,
        "no retired local diagnostic may pin the actual frame after its external receiver returns"
    );
    let delivered = original
        .driver
        .join()
        .await
        .expect_err("past execution failure remains explicit after original payload returns");
    assert!(delivered.to_string().contains("already handed"));
    assert!(
        delivered
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .is_none(),
        "do not fabricate an original source after it was released"
    );
    drop(delivered);
    // No new dial, scope, reaper or fresh book is needed to unpin/shrink.
    book.prepare(target).unwrap().commit().unwrap();
    assert_eq!(book.limits(), target);
    let next = Caller::begin(Limits {
        frame: 64,
        total: 128,
    })
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&next.original.book, &book));
    next.finish().await.unwrap();
}
