//! Real HTTP/1 response EOF leaves its original upload pending until wire EOF.
//! These exchange tests make no Host/provider close-receipt claim.
use super::*;
use crate::split_http::{io::LifetimeRef, sessions::Sessions};
use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ObservedBody {
    incoming: hyper::body::Incoming,
    life: Arc<Mutex<Option<LifetimeRef>>>,
    phase: tokio::sync::watch::Sender<u8>,
    received: bool,
    failed: Arc<AtomicBool>,
}
impl hyper::body::Body for ObservedBody {
    type Data = Bytes;
    type Error = hyper::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        let result = Pin::new(&mut self.incoming).poll_frame(cx);
        if matches!(&result, Poll::Ready(Some(Err(_)))) {
            self.failed.store(true, Ordering::Release);
        }
        if matches!(&result, Poll::Ready(Some(Ok(frame))) if frame.is_data()) {
            self.received = true;
        }
        if result.is_pending() && self.received {
            let returned = self
                .life
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|life| life.is_closed());
            self.phase.send_replace(if returned { 2 } else { 1 });
        } else if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.phase.send_replace(3);
        }
        result
    }
}
async fn line(client: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n") {
        bytes.push(client.read_u8().await.unwrap());
        assert!(bytes.len() <= 8192);
    }
    bytes
}
async fn response(client: &mut tokio::net::TcpStream) -> Vec<u8> {
    assert!(line(client).await.starts_with(b"HTTP/1.1 200"));
    while line(client).await != b"\r\n" {}
    let mut payload = Vec::new();
    loop {
        let header = line(client).await;
        let count =
            usize::from_str_radix(std::str::from_utf8(&header).unwrap().trim(), 16).unwrap();
        if count == 0 {
            assert_eq!(line(client).await, b"\r\n");
            return payload;
        }
        let previous = payload.len();
        payload.resize(previous + count, 0);
        client.read_exact(&mut payload[previous..]).await.unwrap();
        assert_eq!(line(client).await, b"\r\n");
    }
}

async fn actual_stream_one_return(malformed: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let life = Arc::new(Mutex::new(None));
        let failed = Arc::new(AtomicBool::new(false));
        let wire_failed = failed.clone();
        let original = life.clone();
        let (phase, mut waiting) = tokio::sync::watch::channel(0);
        let (accepted, mut incoming) = mpsc::channel(1);
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let profile = Profile::new(&crate::profile::OwnedSplitHttpProfile {
            host: None, path: "/upload/".into(), mode: "stream-one".into(),
            options: zero_traits::SplitHttpOptions { x_padding_bytes: zero_traits::SplitHttpRange::new(1,1), no_sse_header: true, ..Default::default() },
        });
        let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
            let life = life.clone(); let phase = phase.clone(); let profile = profile.clone(); let failed = failed.clone();
            let accepted = accepted.clone(); let permit = permits.clone().try_acquire_owned().unwrap();
            async move {
                super::super::exchange::handle(request.map(|incoming| ObservedBody { incoming, life, phase, received: false, failed }), profile, Sessions::default(), accepted, permit).await
            }
        });
        let server = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new().half_close(true).serve_connection(TokioIo::new(peer), service).await
        });
        client.write_all(b"POST /upload/?x_padding=X HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n").await.unwrap();
        let mut stream = incoming.recv().await.unwrap();
        let actual_life = stream.life.clone();
        *original.lock().unwrap() = Some(actual_life.clone());
        let mut payload = [0; 3]; stream.read_exact(&mut payload).await.unwrap(); assert_eq!(&payload, b"abc");
        waiting.wait_for(|phase| *phase == 1).await.unwrap();
        stream.write_all(b"xyz").await.unwrap(); stream.shutdown().await.unwrap();
        assert_eq!(response(&mut client).await, b"xyz");
        waiting.wait_for(|phase| *phase == 2).await.unwrap();
        assert!(!server.is_finished(), "the actual original request body is still pending");
        drop(stream);
        client.write_all(if malformed { b"invalid\r\n" } else { b"3\r\ndef\r\n0\r\n\r\n" }).await.unwrap();
        waiting.wait_for(|phase| *phase == 3).await.unwrap();
        if malformed {
            client.shutdown().await.unwrap();
            let _dispatcher = server.await.unwrap();
            assert!(wire_failed.load(Ordering::Acquire), "actual HTTP parser delivered the malformed body error");
            assert!(actual_life.error().is_err(), "actual malformed upload after response EOF remains an error");
        } else {
            assert!(!wire_failed.load(Ordering::Acquire));
            actual_life.error().unwrap();
            client.write_all(b"OPTIONS /upload/ HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n").await.unwrap();
            let mut next = Vec::new(); client.read_to_end(&mut next).await.unwrap();
            assert!(next.starts_with(b"HTTP/1.1 200"), "same native connection remains reusable");
            server.await.unwrap().unwrap();
        }
    }).await.unwrap();
}
#[tokio::test]
async fn stream_one_response_eof_drains_actual_upload_before_reusing_http1() {
    actual_stream_one_return(false).await;
}
#[tokio::test]
async fn malformed_stream_one_upload_after_response_eof_remains_a_wire_error() {
    actual_stream_one_return(true).await;
}
