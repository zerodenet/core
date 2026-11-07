use super::*;
use std::{future::Future, task::Poll};
use zero_runtime::configuration::ControlBudgetLimits;
#[tokio::test]
async fn actual_pending_http_write_holds_exact_wire_envelope_and_shrink_is_busy() {
    let limits = ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    let budget = ControlBudget::new(limits);
    let (mut server, mut client) = tokio::io::duplex(1);
    let body = body::HttpBody::Query(Ok(zero_api::QueryResponse::Unknown(
        serde_json::json!({"actual":"same original DTO"}),
    )));
    let length = body.len().unwrap();
    let mut header = Count(0);
    headers("HTTP/1.1 200 OK\r\n", length, &mut header).unwrap();
    let exact = length + header.0;
    let mut sending = Box::pin(write_response(
        &mut server,
        "HTTP/1.1 200 OK\r\n",
        &body,
        &budget,
    ));
    std::future::poll_fn(|cx| {
        assert!(sending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(budget.usage().response_bytes, exact);
    let mut lower = limits;
    lower.messages.response_bytes_total = exact - 1;
    assert!(budget.prepare(lower).is_err());
    let mut bytes = Vec::new();
    let (sent, read) = tokio::join!(sending, client.read_to_end(&mut bytes));
    sent.unwrap();
    read.unwrap();
    assert_eq!(bytes.len(), exact);
    assert_eq!(budget.usage().response_bytes, 0);
    let offset = bytes
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .unwrap()
        + 4;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes[offset..]).unwrap()["result"]["actual"],
        "same original DTO"
    );
}
