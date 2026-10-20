//! Real TCP/H2 frames establish the liveness boundary before advancing time.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[derive(Default)]
struct Frames {
    bytes: Mutex<Vec<u8>>,
    preface: AtomicBool,
    last_read: Mutex<Option<tokio::time::Instant>>,
    ping: AtomicBool,
}
impl Frames {
    fn accepted(&self, bytes: &[u8]) {
        let mut pending = self.bytes.lock().unwrap();
        pending.extend_from_slice(bytes);
        if !self.preface.load(Ordering::Acquire) {
            if pending.len() < 24 {
                return;
            }
            assert_eq!(&pending[..24], b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            pending.drain(..24);
            self.preface.store(true, Ordering::Release);
        }
        while pending.len() >= 9 {
            let size = ((pending[0] as usize) << 16)
                | ((pending[1] as usize) << 8)
                | (pending[2] as usize);
            if pending.len() < 9 + size {
                break;
            }
            if size == 8 && pending[3] == 6 && pending[4] == 0 {
                self.ping.store(true, Ordering::Release);
            }
            pending.drain(..9 + size);
        }
    }
}
struct Observed {
    io: tokio::net::TcpStream,
    frames: Arc<Frames>,
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if buf.filled().len() > before {
            *self.frames.last_read.lock().unwrap() = Some(tokio::time::Instant::now());
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(n)) = result {
            self.frames.accepted(&bytes[..n]);
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
async fn actual_h2_ping_timeout_keeps_original_elapsed_source_and_original_minimum_and_default_bounds(
) {
    for configured in [1, 0] {
        run(configured).await;
    }
}
async fn run(configured: u32) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (hold, held) = tokio::sync::oneshot::channel();
    let (ack, acknowledged) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await.unwrap();
        let (request, mut respond) = connection.accept().await.unwrap().unwrap();
        assert!(request.body().is_end_stream());
        respond
            .send_response(
                Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        tokio::select! {_=held=>{},_=connection.accept()=>panic!("only the actual first request was submitted")}
        // Keep the original H2 connection/socket alive, but deliberately stop its
        // driver after real response headers were received. It cannot ACK the ping.
        ack.send(()).unwrap();
        finished.await.unwrap();
        drop(connection);
    });
    let frames = Arc::new(Frames::default());
    let io = tokio::net::TcpStream::connect(address).await.unwrap();
    let profile = OwnedGrpcProfile {
        service_names: vec!["liveness".into()],
        idle_timeout_secs: 1,
        health_check_timeout_secs: configured,
        permit_without_stream: true,
        ..Default::default()
    };
    let crate::grpc::prepared::PreparedConnection {
        sender,
        driver,
        active: _,
    } = crate::grpc::prepared::prepare_connection(
        Observed {
            io,
            frames: frames.clone(),
        },
        profile.clone(),
        None,
    )
    .await
    .unwrap();
    tokio::pin!(driver);
    let ready = async move {
        let mut sender = sender.ready().await.unwrap();
        let (response, send) = sender
            .send_request(options::request(&profile, "localhost").unwrap(), true)
            .unwrap();
        drop(send);
        let response = response.await.unwrap();
        assert_eq!(response.status(), 200);
        drop(response);
        hold.send(()).unwrap();
        acknowledged.await.unwrap();
        sender
    };
    let sender=timeout(Duration::from_secs(3),async {tokio::select!{result=ready=>result,_=driver.as_mut()=>panic!("original H2 driver exited before real headers")}}).await.unwrap();
    tokio::time::pause();
    let last = frames.last_read.lock().unwrap().unwrap();
    tokio::time::advance(
        (last + Duration::from_secs(9)).saturating_duration_since(tokio::time::Instant::now()),
    )
    .await;
    std::future::poll_fn(|cx| {
        assert!(driver.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        !frames.ping.load(Ordering::Acquire),
        "client idle interval retains its ten-second minimum"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..1024 {
        if frames.ping.load(Ordering::Acquire) {
            break;
        }
        std::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if frames.ping.load(Ordering::Acquire) {
            break;
        }
        // Any real late SETTINGS/ACK read restarts the original idle clock.
        // Advance from that observed read, not an assumed packet-drain delay.
        let last = frames.last_read.lock().unwrap().unwrap();
        let remaining =
            (last + Duration::from_secs(10)).saturating_duration_since(tokio::time::Instant::now());
        if !remaining.is_zero() {
            tokio::time::advance(remaining).await;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        frames.ping.load(Ordering::Acquire),
        "the real native write accepted a complete H2 PING frame"
    );
    let seconds = if configured == 0 { 20 } else { configured };
    if seconds > 1 {
        tokio::time::advance(Duration::from_secs((seconds - 1) as u64)).await;
        std::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    let failure = driver.as_mut().await.unwrap_err();
    assert_eq!(failure.kind(), io::ErrorKind::TimedOut);
    assert!(
        failure
            .get_ref()
            .unwrap()
            .is::<tokio::time::error::Elapsed>(),
        "the actual timeout error object is retained"
    );
    tokio::time::resume();
    drop(sender);
    finish.send(()).unwrap();
    server.await.unwrap();
    // This caller-selected prepared seam owns no controlled fd and asserts no
    // physical close receipt merely because its driver future returned.
}
