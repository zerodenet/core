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
    headers: usize,
}
impl AsyncWrite for ObservedWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.io).poll_write(cx, bytes);
        match &polled {
            Poll::Ready(Ok(written)) => self.headers = self.headers.saturating_sub(*written),
            Poll::Pending if self.headers == 0 => {
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
        let (pending, writing) = tokio::sync::oneshot::channel();
        let server = ObservedWrite {
            io: server,
            pending: Some(pending),
            headers: super::super::SSE_HEADERS.len(),
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
        loop {
            let mut line = String::new();
            assert!(client.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        for index in 0..3 {
            engine.emit(RawApiEvent::new(
                format!("same-{index}"),
                "test.local-frame",
                0,
                json!({"payload":"a".repeat(128*1024)}),
            ));
        }
        writing.await.unwrap(); // The actual original TCP writer returned Pending.
        let mut lower = limits;
        lower.messages.sse_event_frames = 1;
        loop {
            match budget.prepare(lower) {
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                    break;
                }
                Ok(candidate) => candidate.retire().unwrap(),
            }
            tokio::task::yield_now().await;
        }
        assert!(budget.usage().queued_event_bytes > 0);
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
    .unwrap();
}

#[tokio::test]
async fn ordered_catchup_reserves_its_real_working_frame_before_the_same_worker_queue() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let engine = engine();
        let subscriber = engine
            .subscribe(EventFilter {
                event_types: vec!["test.ordered".into()],
                ..Default::default()
            })
            .unwrap();
        let mut caught = RawApiEvent::new("caught", "test.ordered", 0, json!({"order":0}));
        caught.sequence = Some(1);
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
