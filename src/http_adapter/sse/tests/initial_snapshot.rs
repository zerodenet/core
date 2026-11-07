//! Original history, prepaid initial and live frames share one ordered slot.
use super::super::{
    run_sse_stream_with_budget, CancellationToken, ControlBudget, ControlBudgetLimits,
    ControlCharge,
};
use super::*;
use std::{
    future::{poll_fn, Future},
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncReadExt, AsyncWrite};
struct PendingWrite {
    io: tokio::io::DuplexStream,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AsyncWrite for PendingWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if result.is_pending() {
            if let Some(signal) = self.pending.take() {
                let _ = signal.send(());
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn prepaid_original_snapshot_and_replay_keep_n1_order_through_actual_pending_write() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let handle = zero_proxy::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        let engine = handle.engine_handle();
        engine.emit(zero_api::ApiEvent::new(
            "same-replay",
            "fixture.replay",
            1,
            serde_json::json!({"original":17,"padding":"p".repeat(512)}),
        ));
        let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
        limits.messages.sse_event_frames = 1;
        let book = ControlBudget::new(limits);
        let connection = book.reserve(ControlCharge::Connection).unwrap();
        let subscription = book.reserve(ControlCharge::Subscription).unwrap();
        let local = connection.connection_budget().unwrap();
        let (source, replay) =
            crate::http_adapter::handlers::events_stream(&handle, "since=0", &[], &book, &local)
                .unwrap_or_else(|_| {
                    panic!("actual source must admit the initial ordered operation")
                });
        assert!(source.initial.is_some() && source.catchup_credit.is_some());
        let held = book.usage().queued_event_bytes;
        let mut limits = book.limits();
        limits.queued_event_bytes = held;
        book.prepare(limits).unwrap().commit().unwrap();
        assert!(local
            .reserve(zero_runtime::configuration::ControlConnectionCharge::SseFrame)
            .is_err());
        engine.emit(zero_api::ApiEvent::new(
            "same-live",
            "fixture.live",
            2,
            serde_json::json!({"original":19}),
        ));
        let (write, mut read) = tokio::io::duplex(1);
        let (signal, mut pending) = tokio::sync::oneshot::channel();
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let mut serving = Box::pin(run_sse_stream_with_budget(
            PendingWrite {
                io: write,
                pending: Some(signal),
            },
            source,
            replay,
            Duration::from_secs(30),
            shutdown.cancelled_owned(),
            book.clone(),
            local.clone(),
        ));
        poll_fn(|cx| {
            assert!(serving.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        pending
            .try_recv()
            .expect("actual original write was polled Pending");
        assert_eq!(book.usage().queued_event_bytes, held);
        let task = tokio::spawn(serving);
        let mut wire = Vec::new();
        let mut byte = [0; 1];
        let marker = b"event: fixture.live\n";
        while !(wire.ends_with(b"\n\n") && wire.windows(marker.len()).any(|v| v == marker)) {
            assert_eq!(read.read(&mut byte).await.unwrap(), 1);
            wire.push(byte[0]);
        }
        let text = std::str::from_utf8(&wire).unwrap();
        let replay = text.find("event: fixture.replay\n").unwrap();
        let initial = text.find("event: flow.snapshot\n").unwrap();
        let live = text.find("event: fixture.live\n").unwrap();
        assert!(replay < initial && initial < live);
        stop.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(book.usage().queued_event_bytes, 0);
        assert_eq!(book.usage().response_bytes, 0);
        let slot = local
            .reserve(zero_runtime::configuration::ControlConnectionCharge::SseFrame)
            .unwrap();
        drop(slot);
        drop(subscription);
        drop(connection);
        book.close_admission();
        book.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
