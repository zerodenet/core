use super::*;
use std::{future::Future, task::Poll};
use tokio::io::AsyncWriteExt;
fn budget(frame: usize) -> Arc<ControlBudget> {
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.ipc_frame_bytes = frame;
    limits.messages.request_bytes_total = 32;
    limits.messages.response_bytes_total = 32;
    ControlBudget::new(limits)
}
#[tokio::test]
async fn original_partial_frame_is_charged_until_its_buffer_is_released() {
    let budget = budget(8);
    let (mut writer, reader) = tokio::io::duplex(32);
    let mut reader = BufReader::new(reader);
    writer.write_all(b"1234").await.unwrap();
    let mut reading = Box::pin(read_request_frame(&mut reader, &budget));
    std::future::poll_fn(|cx| {
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(budget.usage().request_bytes, 4);
    writer.write_all(b"5\n").await.unwrap();
    let frame = reading.await.unwrap().unwrap();
    assert_eq!(frame.bytes(), b"12345\n");
    assert_eq!(budget.usage().request_bytes, 6);
    let decoded = frame.reserve_decoded(&budget).unwrap();
    assert_eq!(budget.usage().request_bytes, 12);
    drop(frame);
    assert_eq!(budget.usage().request_bytes, 6);
    drop(decoded);
    assert_eq!(budget.usage().request_bytes, 0);
}
#[tokio::test]
async fn oversized_original_input_stops_before_allocating_the_excess_frame() {
    let budget = budget(4);
    let (mut writer, reader) = tokio::io::duplex(32);
    let mut reader = BufReader::new(reader);
    writer.write_all(b"12345\n").await.unwrap();
    let error = read_request_frame(&mut reader, &budget)
        .await
        .err()
        .unwrap();
    assert!(input::is_invalid_input_frame(&error));
    assert_eq!(budget.usage().request_bytes, 0);
}
#[tokio::test]
async fn actual_response_credit_precedes_pending_write_and_lasts_through_flush() {
    let budget = budget(8);
    let (mut writer, mut reader) = tokio::io::duplex(1);
    let value = ApiResponse::ok_with_id(None, "actual");
    let bytes = super::super::super::owner::serialized_len(&value).unwrap() + 1;
    // This real envelope needs more than the small helper's default total.
    let budget = ControlBudget::new(ControlBudgetLimits {
        messages: zero_runtime::configuration::ControlMessageLimits {
            response_bytes_total: 1024,
            ..budget.limits().messages
        },
        ..budget.limits()
    });
    let mut sending = Box::pin(write_budgeted_response(&mut writer, &value, &budget));
    std::future::poll_fn(|cx| {
        assert!(sending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(budget.usage().response_bytes, bytes);
    let mut result = vec![0; bytes];
    let (sent, received) = tokio::join!(
        sending,
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut result)
    );
    sent.unwrap();
    received.unwrap();
    assert_eq!(budget.usage().response_bytes, 0);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result).unwrap()["result"],
        "actual"
    );
}

#[tokio::test]
async fn original_next_frame_stays_pending_while_completed_command_join_returns_root_credit() {
    let budget = budget(32);
    let stop = CancellationToken::new();
    let mut commands = ConcurrentCommandTasks::test_with_budget(stop, budget.clone());
    let (completed, body) = tokio::sync::oneshot::channel();
    assert!(commands.try_spawn(async move {
        completed.send(()).unwrap();
        Ok(())
    }));
    body.await.unwrap();
    let (mut writer, reader) = tokio::io::duplex(32);
    let mut reader = BufReader::new(reader);
    writer.write_all(b"same ").await.unwrap();
    let mut reading = Box::pin(next_request(&mut reader, &budget, &mut commands));
    while budget.usage().pending_commands != 0 {
        std::future::poll_fn(|cx| {
            assert!(reading.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tokio::task::yield_now().await;
    }
    assert_eq!(budget.usage().request_bytes, 5);
    writer.write_all(b"original\n").await.unwrap();
    let frame = reading.await.unwrap().unwrap();
    assert_eq!(frame.bytes(), b"same original\n");
    drop(frame);
    assert_eq!(budget.usage().request_bytes, 0);
    commands.cancel_all().await.unwrap();
}
