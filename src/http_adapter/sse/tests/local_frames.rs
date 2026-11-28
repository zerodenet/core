//! A real pending TCP write retains the local frame and original worker credit.
use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
struct ObservedWrite {
    io: TcpStream,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
    frame_written: std::sync::Arc<tokio::sync::Notify>,
    headers: usize,
    suspended_after_pending: bool,
}
impl AsyncWrite for ObservedWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.suspended_after_pending {
            // Keep this same write future at its observed suspension until shutdown
            // drops it. This does not claim that the OS socket stays unwritable.
            return Poll::Pending;
        }
        let is_body = self.headers == 0;
        let polled = Pin::new(&mut self.io).poll_write(cx, bytes);
        match &polled {
            Poll::Ready(Ok(written)) => {
                if is_body && !bytes.is_empty() && *written == bytes.len() {
                    // write_all has no remaining bytes in this real body frame.
                    self.frame_written.notify_one();
                }
                self.headers = self.headers.saturating_sub(*written);
            }
            Poll::Pending if self.headers == 0 => {
                // Arm only after the original TCP write actually returned Pending.
                self.suspended_after_pending = true;
                if let Some(pending) = self.pending.take() {
                    let _ = pending.send(());
                }
            }
            _ => {}
        }
        polled
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn sse_original_frame_write_and_queue_hold_shrink_busy_until_actual_worker_join() {
    let phase = std::cell::Cell::new("connect original loopback TCP");
    let emitted = std::cell::Cell::new(0);
    tokio::time::timeout(Duration::from_secs(3), async {
        let engine = engine();
        let subscriber = engine
            .subscribe(EventFilter {
                event_types: vec!["test.local-frame".into()],
                ..Default::default()
            })
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        socket2::SockRef::from(&client)
            .set_recv_buffer_size(1024)
            .unwrap();
        socket2::SockRef::from(&server)
            .set_send_buffer_size(1024)
            .unwrap();
        let mut limits =
            ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
        limits.messages.sse_event_frames = 2;
        let budget = ControlBudget::new(limits);
        let connection = budget.reserve(ControlCharge::Connection).unwrap();
        let local = connection.connection_budget().unwrap();
        let (pending, mut writing) = tokio::sync::oneshot::channel();
        let frame_written = std::sync::Arc::new(tokio::sync::Notify::new());
        let server = ObservedWrite {
            io: server,
            pending: Some(pending),
            frame_written: frame_written.clone(),
            headers: super::super::SSE_HEADERS.len(),
            suspended_after_pending: false,
        };
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
        phase.set("read original SSE headers");
        loop {
            let mut line = String::new();
            assert!(client.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        phase.set("observe original TCP write Pending while pumping real frames");
        // At most 16 MiB of real payload, stopping at the first actual Pending.
        // Await each real frame completion before emitting another so subscriber
        // admission cannot silently drop a fast fixed burst instead of filling TCP.
        const MAX_PUMP_EVENTS: usize = 128;
        let mut observed_pending = false;
        for index in 0..MAX_PUMP_EVENTS {
            engine.emit(RawApiEvent::new(
                format!("same-{index}"),
                "test.local-frame",
                0,
                json!({"payload":"a".repeat(128*1024)}),
            ));
            emitted.set(index + 1);
            tokio::select! {
                biased;
                result = &mut writing => {
                    result.expect("the original writer must report its actual Pending");
                    observed_pending = true;
                    break;
                }
                _ = frame_written.notified() => {}
            }
        }
        assert!(
            observed_pending,
            "original TCP accepted all {MAX_PUMP_EVENTS} real frames without Pending"
        );
        phase.set("observe two real local frame reservations");
        // A frame was emitted only after its predecessor completed on the real TCP.
        // Queue a real event behind that suspended original, not a synthetic credit.
        engine.emit(RawApiEvent::new(
            "queued-after-original-pending",
            "test.local-frame",
            0,
            json!({"payload":"b".repeat(128*1024)}),
        ));
        let mut lower = limits;
        lower.messages.sse_event_frames = 1;
        loop {
            // Preparing a candidate would temporarily cap the second frame at one.
            // Observe the actual holders without changing their admission limits.
            if let Err(error) = budget.validate(lower) {
                assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(budget.prepare(lower), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
        assert!(budget.usage().queued_event_bytes > 0);
        phase.set("cancel and join the original SSE worker");
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(budget.usage().queued_event_bytes, 0);
        drop(client);
        drop(connection);
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "{error}: SSE fixture phase: {}; emitted {} real frames",
            phase.get(),
            emitted.get()
        )
    });
}

#[tokio::test]
async fn ordered_catchup_reserves_its_real_working_frame_before_the_same_worker_queue() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let engine = engine();
        let filter = EventFilter {
            event_types: vec!["test.ordered".into()],
            ..Default::default()
        };
        engine.emit(RawApiEvent::new(
            "caught",
            "test.ordered",
            0,
            json!({"order":0}),
        ));
        // Production catch-up already carries this Engine's published metadata.
        // Measure that same real envelope, not an unqualified synthetic event.
        let caught = engine.latest(1, filter.clone()).unwrap().pop().unwrap();
        assert!(caught.core_instance_id.is_some());
        assert!(caught.config_revision.is_some());
        let subscriber = engine.subscribe(filter).unwrap();
        let mut limits =
            ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
        limits.messages.sse_event_frames = 1;
        limits.queued_event_bytes = super::super::frame_len(&caught).unwrap() + 32;
        let budget = ControlBudget::new(limits);
        let connection = budget.reserve(ControlCharge::Connection).unwrap();
        let local = connection.connection_budget().unwrap();
        // A genuinely ready subscriber event competes with the existing catch-up.
        engine.emit(RawApiEvent::new(
            "later",
            "test.ordered",
            0,
            json!({"order":1}),
        ));
        let (server, client) = tokio::io::duplex(32);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(super::super::run_sse_stream_with_budget(
            server,
            subscriber,
            vec![caught],
            Duration::from_secs(30),
            stopped,
            budget.clone(),
            local,
        ));
        let mut client = BufReader::new(client);
        let mut actual = Vec::new();
        while actual.len() < 2 {
            let mut line = String::new();
            assert!(client.read_line(&mut line).await.unwrap() > 0);
            if let Some(json) = line.strip_prefix("data: ") {
                actual.push(
                    serde_json::from_str::<RawApiEvent>(json).unwrap().payload["order"]
                        .as_u64()
                        .unwrap(),
                );
            }
        }
        assert_eq!(actual, [0, 1]);
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(budget.usage().queued_event_bytes, 0);
        drop(client);
        drop(connection);
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
