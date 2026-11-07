use super::*;
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug)]
struct EventWriteFailure(Arc<()>);
impl std::fmt::Display for EventWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original IPC event write denied")
    }
}
impl std::error::Error for EventWriteFailure {}
struct FailingEvents {
    io: tokio::io::DuplexStream,
    fail: Arc<AtomicBool>,
    original: Arc<()>,
}
impl AsyncRead for FailingEvents {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, bytes)
    }
}
impl AsyncWrite for FailingEvents {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fail.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                EventWriteFailure(self.original.clone()),
            )));
        }
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn non_disconnect_event_write_failure_keeps_its_typed_source_after_actual_bridge_join() {
    use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits};
    let (engine, handle) = handles();
    let (client, server) = tokio::io::duplex(256);
    let fail = Arc::new(AtomicBool::new(false));
    let original = Arc::new(());
    let budget = ControlBudget::new(ControlBudgetLimits {
        connections: 1,
        subscriptions: 1,
        queued_event_bytes: 1024 * 1024,
        pipe_workspace_bytes: 2 * 1024 * 1024,
        messages: Default::default(),
    });
    let server = FailingEvents {
        io: server,
        fail: fail.clone(),
        original: original.clone(),
    };
    let task = tokio::spawn(
        crate::ipc::connection::handle_ipc_connection_with_budget_until(
            server,
            handle,
            zero_platform_tokio::contract::CancellationToken::new(),
            budget.clone(),
        ),
    );
    let mut client = BufReader::new(client);
    client
        .get_mut()
        .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.failure\"]}\n")
        .await
        .unwrap();
    assert_eq!(read_frame(&mut client).await["ok"], true);
    fail.store(true, Ordering::Release);
    engine.emit(RawApiEvent::new(
        "original-write-failure",
        "test.failure",
        0,
        json!({"value":1}),
    ));
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let retained = error
        .get_ref()
        .unwrap()
        .downcast_ref::<EventWriteFailure>()
        .expect("original typed writer failure");
    assert!(Arc::ptr_eq(&retained.0, &original));
    // The original blocking bridge and writer were both joined before these
    // charges returned. These counters make no native-close claim.
    assert_eq!(budget.usage().subscriptions, 0);
    assert_eq!(budget.usage().queued_event_bytes, 0);
}
