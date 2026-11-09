use super::*;
use std::{future::Future, task::Poll};
use zero_runtime::configuration::ControlBudgetLimits;
#[tokio::test]
async fn real_partial_http_body_keeps_original_buffers_and_decoded_credit_until_return() {
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.http_header_bytes = 128;
    limits.messages.http_body_bytes = 4;
    let budget = ControlBudget::new(limits);
    let (mut sender, mut receiver) = tokio::io::duplex(256);
    sender
        .write_all(b"POST /api/v1/query HTTP/1.1\r\nContent-Length: 4\r\n\r\nab")
        .await
        .unwrap();
    let mut reading = Box::pin(read_request(&mut receiver, &budget));
    std::future::poll_fn(|cx| {
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(budget.usage().request_bytes >= 128 + 8);
    sender.write_all(b"cd").await.unwrap();
    let request = reading.await.unwrap();
    assert_eq!(request.body, b"abcd");
    assert_eq!(request.method, "POST");
    assert!(budget.usage().request_bytes >= 8);
    let mut lower = limits;
    lower.messages.request_bytes_total = 1;
    assert!(budget.prepare(lower).is_err());
    drop(request);
    assert_eq!(budget.usage().request_bytes, 0);
    budget.prepare(lower).unwrap().retire().unwrap();
}
#[tokio::test]
async fn actual_http_body_cap_rejects_before_allocating_announced_payload() {
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.http_body_bytes = 1;
    let budget = ControlBudget::new(limits);
    let (mut sender, mut receiver) = tokio::io::duplex(256);
    sender
        .write_all(b"POST /api/v1/query HTTP/1.1\r\nContent-Length: 999999\r\n\r\n")
        .await
        .unwrap();
    let error = read_request(&mut receiver, &budget).await.err().unwrap();
    assert!(client_frame(&error));
    assert_eq!(budget.usage().request_bytes, 0);
}

#[tokio::test]
async fn actual_http_header_rows_keep_paid_capacity_order_and_duplicates_until_request_return() {
    let limits = ControlBudgetLimits::from_runtime(&Default::default());
    let book = ControlBudget::new(limits);
    let wire = b"GET /health HTTP/1.1\r\nX:a\r\nignored\r\nX:b\r\n\r\n";
    let (mut sender, mut receiver) = tokio::io::duplex(256);
    sender.write_all(wire).await.unwrap();
    let request = read_request(&mut receiver, &book).await.unwrap();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/health");
    assert_eq!(
        request.headers,
        &[
            ("X".to_owned(), "a".to_owned()),
            ("X".to_owned(), "b".to_owned())
        ]
    );
    assert_eq!(request.headers.capacity(), 2);
    assert_eq!(
        book.usage().request_bytes,
        wire.len() + 2 * std::mem::size_of::<(String, String)>()
    );
    let mut lower = limits;
    lower.messages.request_bytes_total = wire.len();
    assert!(
        book.prepare(lower).is_err(),
        "held concrete header metadata cannot be ignored by shrink"
    );
    drop(request);
    assert_eq!(book.usage().request_bytes, 0);
    book.prepare(lower).unwrap().retire().unwrap();
}
#[tokio::test]
async fn actual_http_header_metadata_refuses_before_copying_and_failed_admission_leaves_no_bytes() {
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.http_header_bytes = 64;
    let wire = b"GET /health HTTP/1.1\r\nX:a\r\nX:b\r\n\r\n";
    limits.messages.request_bytes_total = 64 + wire.len();
    let book = ControlBudget::new(limits);
    let (mut sender, mut receiver) = tokio::io::duplex(256);
    sender.write_all(wire).await.unwrap();
    let error = read_request(&mut receiver, &book).await.err().unwrap();
    assert!(
        matches!(error.get_ref().and_then(|source|source.downcast_ref::<ClientFrame>()),Some(ClientFrame::Capacity(source)) if source.kind()==io::ErrorKind::WouldBlock)
    );
    assert_eq!(book.usage().request_bytes, 0);
    let mut target = limits;
    target.messages.request_bytes_total += 2 * std::mem::size_of::<(String, String)>();
    book.prepare(target).unwrap().commit().unwrap();
    sender.write_all(wire).await.unwrap();
    let request = read_request(&mut receiver, &book).await.unwrap();
    assert_eq!(request.headers.len(), 2);
    drop(request);
    assert_eq!(book.usage().request_bytes, 0);
}

#[tokio::test]
async fn actual_json_body_tree_capacity_is_held_by_the_original_http_request() {
    let limits = ControlBudgetLimits::from_runtime(&Default::default());
    let book = ControlBudget::new(limits);
    let body = br#"{"health":{},"ignored":[{},{},{},{},{},{},{},{}]}"#;
    let wire = format!(
        "POST /api/v1/query HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let (mut tx, mut rx) = tokio::io::duplex(1024);
    tx.write_all(wire.as_bytes()).await.unwrap();
    tx.write_all(body).await.unwrap();
    let held = Arc::new(read_request(&mut rx, &book).await.unwrap());
    assert_eq!(held.body, body);
    assert!(book.usage().request_bytes > body.len() * 8);
    let actual_work = held.clone();
    drop(held);
    let mut shrink = limits;
    shrink.messages.request_bytes_total = 1;
    assert!(book.prepare(shrink).is_err());
    drop(actual_work);
    assert_eq!(book.usage().request_bytes, 0);
    book.prepare(shrink).unwrap().commit().unwrap();
}
