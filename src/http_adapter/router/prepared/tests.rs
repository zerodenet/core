use super::*;
use zero_api::QueryService;
fn fixture() -> (ProxyHandle, Arc<ControlBudget>) {
    let proxy=zero_proxy::Proxy::new(zero_config::RuntimeConfig::parse(r#"{
        "outbounds":[{"tag":"a","protocol":{"type":"direct"}},{"tag":"long-雪","protocol":{"type":"direct"}}],
        "outbound_groups":[{"tag":"pick","type":"selector","outbounds":["a","long-雪"]}],
        "route":{"final":{"type":"direct"}}
    }"#).unwrap()).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy,
    );
    let budget = ControlBudget::new(
        zero_runtime::configuration::ControlBudgetLimits::from_runtime(
            &zero_config::RuntimeOptionsConfig::default(),
        ),
    );
    (handle, budget)
}
#[test]
fn prepared_http_config_has_same_flat_wire_and_releases_engine_guards_before_native_write() {
    let (handle, budget) = fixture();
    let query = QueryRequest::Config(Default::default());
    let expected = handlers::serialize_query(handle.query(query))
        .unwrap()
        .bytes_for_test();
    let original = prepare(&handle, Selected::Config, &budget).unwrap();
    assert_eq!(original.bytes_for_test(), expected);
    assert_eq!(budget.usage().response_bytes, original.len().unwrap());
    handle
        .engine_handle()
        .inner()
        .set_selector_target("pick", "long-雪")
        .unwrap();
    assert_eq!(original.bytes_for_test(), expected);
    drop(original);
    assert_eq!(budget.usage().response_bytes, 0);
    assert_eq!(budget.usage().request_bytes, 0);
    let missing = prepare(&handle, Selected::Policy("absent-雪"), &budget).unwrap();
    let expected =
        handlers::serialize_query(handle.query(QueryRequest::Policy(zero_api::PolicyGetQuery {
            policy_tag: "absent-雪".into(),
        })))
        .unwrap();
    assert_eq!(missing.bytes_for_test(), expected.bytes_for_test());
    drop(missing);
    assert_eq!(budget.usage().response_bytes, 0);
    assert_eq!(budget.usage().request_bytes, 0);
}
#[tokio::test]
async fn actual_prepared_http_write_keeps_same_body_credit_through_real_pending_and_shutdown() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (handle, budget) = fixture();
    let body = prepare(&handle, Selected::Config, &budget).unwrap();
    let body_bytes = body.len().unwrap();
    let (mut sending, mut receiving) = tokio::io::duplex(1);
    let mut write = Box::pin(crate::http_adapter::output::write_response(
        &mut sending,
        "HTTP/1.1 200 OK\r\n",
        &body,
        &budget,
    ));
    poll_fn(|cx| {
        assert!(write.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        budget.usage().response_bytes > body_bytes,
        "same actual pending header/body owners remain charged"
    );
    handle
        .engine_handle()
        .inner()
        .set_selector_target("pick", "long-雪")
        .unwrap();
    let read = async {
        let mut wire = Vec::new();
        receiving.read_to_end(&mut wire).await.unwrap();
        wire
    };
    let (written, wire) = tokio::join!(write, read);
    written.unwrap();
    assert!(wire.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(wire.ends_with(&body.bytes_for_test()));
    assert_eq!(budget.usage().response_bytes, body_bytes);
    drop(body);
    assert_eq!(budget.usage().response_bytes, 0);
    // The original duplex write-half was shut down; this is independent of
    // returning its payload buffer credit, not an invented native receipt.
    assert!(sending.write_all(b"x").await.is_err());
}

#[tokio::test]
async fn original_prepared_stats_output_stays_frozen_through_actual_pending_io() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    use tokio::io::AsyncReadExt;
    let (handle, budget) = fixture();
    let original = prepare(&handle, Selected::Stats, &budget).unwrap();
    let expected = original.bytes_for_test();
    let output_bytes = original.len().unwrap();
    assert_eq!(budget.usage().response_bytes, output_bytes);
    let (mut tx, mut rx) = tokio::io::duplex(1);
    let mut write = Box::pin(crate::http_adapter::output::write_response(
        &mut tx,
        "HTTP/1.1 200 OK\r\n",
        &original,
        &budget,
    ));
    poll_fn(|cx| {
        assert!(write.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    handle
        .engine_handle()
        .inner()
        .record_udp_upstream_association_created();
    assert_eq!(original.bytes_for_test(), expected);
    let mut limits = budget.limits();
    limits.messages.response_bytes_total = output_bytes - 1;
    match budget.prepare(limits) {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
        Ok(_) => panic!("real original output was refunded before returning"),
    }
    let read = async {
        let mut bytes = Vec::new();
        rx.read_to_end(&mut bytes).await.unwrap();
        bytes
    };
    let (written, wire) = tokio::join!(write, read);
    written.unwrap();
    assert!(wire.ends_with(&expected));
    assert_eq!(budget.usage().response_bytes, output_bytes);
    drop(original);
    assert_eq!(budget.usage().response_bytes, 0);
    let next = prepare(&handle, Selected::Stats, &budget).unwrap();
    assert_ne!(next.bytes_for_test(), expected);
}

#[path = "tests/traffic.rs"]
mod traffic;

#[path = "tests/proxy.rs"]
mod proxy;
