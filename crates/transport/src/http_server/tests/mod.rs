use super::*;
use tokio::io::AsyncWriteExt;

struct PendingHandler {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}
struct Guard(Arc<AtomicBool>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[async_trait::async_trait]
impl HttpHandler for PendingHandler {
    async fn serve(&self, _: http::Request<()>, _: &mut dyn HttpExchange) -> io::Result<()> {
        let _guard = Guard(self.dropped.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
}
#[tokio::test]
async fn cancelling_http_connection_aborts_handler_waiting_for_headers() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let (mut client, server) = tokio::io::duplex(4096);
    let task = tokio::spawn(serve_connection(
        server,
        RequestContext {
            peer: "127.0.0.1:80".parse().unwrap(),
            tls: false,
        },
        Arc::new(PendingHandler {
            entered: entered.clone(),
            dropped: dropped.clone(),
        }),
    ));
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn handler_failure_after_full_response_queue_is_not_successful_eof() {
    use http_body_util::BodyExt;
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(Ok(Bytes::from_static(b"partial")))
        .await
        .unwrap();
    drop(sender);
    let task = tokio::spawn(async {});
    let mut body = ResponseBody {
        receiver,
        task: task.abort_handle(),
        failed: Arc::new(AtomicBool::new(true)),
    };
    assert_eq!(
        body.frame().await.unwrap().unwrap().into_data().unwrap(),
        b"partial"[..]
    );
    assert!(body.frame().await.unwrap().is_err());
    assert!(body.frame().await.is_none());
}
