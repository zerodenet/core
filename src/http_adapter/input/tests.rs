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
