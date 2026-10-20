//! Actual connection-future seams. These do not qualify a pool owner, task
//! budget, H2 executor lifecycle, or controlled physical-close receipt.
use super::*;
use http_body_util::BodyExt;
use std::{
    error::Error,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
};
async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (client, peer)
}
async fn head(peer: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        bytes.push(peer.read_u8().await.unwrap());
        if bytes.ends_with(b"\r\n\r\n") {
            return bytes;
        }
        assert!(bytes.len() < 8192, "bounded fixture request headers");
    }
}
fn request() -> http::Request<Body> {
    http::Request::builder()
        .uri("/prepared")
        .header("host", "fixture.test")
        .body(Body::empty())
        .unwrap()
}
#[tokio::test]
async fn actual_http1_request_and_shutdown_run_in_the_returned_original_driver() {
    let (client, mut peer) = pair().await;
    let PreparedConnection { mut sender, driver } = http1(client).await.unwrap();
    let client = async {
        let response = sender.send_request(request()).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"abc"[..]
        );
        drop(sender);
    };
    let remote = async {
        assert!(head(&mut peer)
            .await
            .starts_with(b"GET /prepared HTTP/1.1\r\n"));
        peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc")
            .await
            .unwrap();
        // Real peer EOF and actual driver completion are separate observations;
        // neither is promoted into a cross-component PhysicalClosed receipt.
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    };
    let (result, (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(driver, client, remote)
    })
    .await
    .unwrap();
    result.unwrap();
}

#[derive(Debug)]
struct CloseSource;
impl std::fmt::Display for CloseSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original injected shutdown source")
    }
}
impl Error for CloseSource {}
struct FailingShutdown {
    io: TcpStream,
    source: Arc<CloseSource>,
}
impl AsyncRead for FailingShutdown {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}
impl AsyncWrite for FailingShutdown {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other(self.source.clone())))
    }
}
#[tokio::test]
async fn returned_http1_driver_retains_the_original_shutdown_failure_object() {
    let (client, mut peer) = pair().await;
    let original = Arc::new(CloseSource);
    let PreparedConnection { mut sender, driver } = http1(FailingShutdown {
        io: client,
        source: original.clone(),
    })
    .await
    .unwrap();
    let client = async {
        let response = sender.send_request(request()).await.unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"abc"[..]
        );
        drop(sender);
    };
    let remote = async {
        head(&mut peer).await;
        peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc")
            .await
            .unwrap();
    };
    let (result, (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(driver, client, remote)
    })
    .await
    .unwrap();
    let error = result.unwrap_err();
    let hyper = error
        .get_ref()
        .unwrap()
        .downcast_ref::<hyper::Error>()
        .expect("original Hyper error is retained, not its text");
    let mut current: Option<&(dyn Error + 'static)> = Some(hyper);
    let mut actual = None;
    while let Some(source) = current {
        if let Some(source) = source.downcast_ref::<Arc<CloseSource>>() {
            actual = Some(source);
            break;
        }
        current = if let Some(source) = source.downcast_ref::<io::Error>() {
            source
                .get_ref()
                .map(|source| source as &(dyn Error + 'static))
        } else {
            source.source()
        };
    }
    assert!(Arc::ptr_eq(
        actual.expect("original shutdown source survives mapping"),
        &original
    ));
}

type Delegated = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Default)]
struct RecordingExecutor(Arc<Mutex<Vec<Delegated>>>);
impl<F> hyper::rt::Executor<F> for RecordingExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(Box::pin(future));
    }
}
#[tokio::test]
async fn http2_handshake_delegates_actual_io_task_to_the_explicit_executor() {
    let (client, mut peer) = pair().await;
    let executor = RecordingExecutor::default();
    let prepared = tokio::time::timeout(
        Duration::from_secs(5),
        http2(client, None, -1, executor.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    // This pinned Hyper version delegates H2ClientFuture::Task at handshake,
    // before its returned outer driver is polled. This executor retains the
    // real future without spawning a Tokio task or fabricating join evidence.
    assert_eq!(executor.0.lock().unwrap().len(), 1);
    let mut preface = [0; 24];
    tokio::time::timeout(Duration::from_secs(5), peer.read_exact(&mut preface))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    drop(prepared.sender);
    drop(prepared.driver);
    assert_eq!(
        executor.0.lock().unwrap().len(),
        1,
        "outer driver Drop cannot claim executor task joined"
    );
    // Test-only destruction releases the retained future. It is deliberately
    // not reported as owner shutdown, actual TaskScope join, or physical ACK.
    executor.0.lock().unwrap().clear();
}
