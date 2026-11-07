//! A small current byte limit backpressures the original subscription.
use super::*;
use std::{future::Future, io, task::Poll};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};

#[tokio::test]
async fn original_subscription_resumes_the_same_event_after_root_byte_limit_grows() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let engine = engine();
        let filter = EventFilter {
            event_types: vec!["test.growth".into()],
            ..Default::default()
        };
        let subscriber = engine.subscribe(filter.clone()).unwrap();
        engine.emit(RawApiEvent::new(
            "retained",
            "test.growth",
            0,
            json!({"original":true}),
        ));
        let event = engine.latest(1, filter).unwrap().pop().unwrap();
        let mut limits =
            ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
        limits.messages.sse_event_frames = 1;
        limits.queued_event_bytes = 1;
        let budget = ControlBudget::new(limits);
        let connection = budget.reserve(ControlCharge::Connection).unwrap();
        let subscription = budget.reserve(ControlCharge::Subscription).unwrap();
        let local = connection.connection_budget().unwrap();
        let (server, client) = tokio::io::duplex(32);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(super::super::run_sse_stream_with_budget(
            server,
            subscriber,
            Vec::new(),
            Duration::from_secs(30),
            stopped,
            budget.clone(),
            local,
        ));
        let mut client = BufReader::new(client);
        loop {
            let mut line = String::new();
            assert!(client.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        let mut line = String::new();
        {
            let read = client.read_line(&mut line);
            tokio::pin!(read);
            std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => {
                    panic!("oversized frame escaped its original book: {result:?}")
                }
            })
            .await;
        }
        assert!(!task.is_finished());
        assert_eq!(budget.usage().queued_event_bytes, 0);
        assert_eq!(budget.usage().subscriptions, 1);
        limits.queued_event_bytes = super::super::frame_len(&event).unwrap();
        budget.prepare(limits).unwrap().commit().unwrap();
        loop {
            line.clear();
            assert!(client.read_line(&mut line).await.unwrap() > 0);
            if let Some(json) = line.strip_prefix("data: ") {
                assert_eq!(serde_json::from_str::<RawApiEvent>(json).unwrap(), event);
                break;
            }
        }
        stop.send(()).unwrap();
        task.await.unwrap().unwrap(); // The same original blocking worker joined.
        assert_eq!(budget.usage().queued_event_bytes, 0);
        drop(client);
        drop(subscription);
        drop(connection);
        budget.close_admission();
        budget.confirm_released().unwrap();
        io::Result::Ok(())
    })
    .await
    .unwrap()
    .unwrap();
}
