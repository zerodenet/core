//! Native HTTP/1 bytes prove logical return retains the original body reader.
//! This low-level exchange test makes no Host/provider close-receipt claim.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ObservedBody {
    incoming: hyper::body::Incoming,
    session: Arc<Session>,
    phase: tokio::sync::watch::Sender<u8>,
    received: bool,
}
impl hyper::body::Body for ObservedBody {
    type Data = Bytes;
    type Error = hyper::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
        let result = Pin::new(&mut self.incoming).poll_frame(cx);
        if matches!(&result, Poll::Ready(Some(Ok(frame))) if frame.is_data()) {
            self.received = true;
        }
        if result.is_pending() && self.received {
            self.phase
                .send_replace(if self.session.life.is_closed() { 2 } else { 1 });
        }
        result
    }
}

async fn actual_upload_return(malformed: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let sessions = super::super::super::sessions::Sessions::default();
        let session = sessions.get_with_limit("original", 8).unwrap();
        let observed = session.clone();
        let (phase, mut waiting) = tokio::sync::watch::channel(0);
        let profile = Profile::new(&crate::profile::OwnedSplitHttpProfile {
            host: None,
            path: "/upload/".into(),
            mode: "stream-up".into(),
            options: zero_traits::SplitHttpOptions {
                x_padding_bytes: zero_traits::SplitHttpRange::new(1, 1),
                no_sse_header: true,
                ..Default::default()
            },
        });
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let (accepted, _pending) = mpsc::channel(1);
        let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
            let body_session = observed.clone();
            let phase = phase.clone();
            let profile = profile.clone();
            let sessions = sessions.clone();
            let accepted = accepted.clone();
            let permit = permits.clone().try_acquire_owned().unwrap();
            async move {
                super::super::exchange::handle(
                    request.map(|incoming| ObservedBody { incoming, session: body_session, phase, received: false }),
                    profile, sessions, accepted, permit,
                ).await
            }
        });
        let server = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new().half_close(true)
                .serve_connection(TokioIo::new(peer), service).await
        });
        client.write_all(b"POST /upload/original?x_padding=X HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/grpc\r\nconnection: close\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n").await.unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(client.read_u8().await.unwrap());
            assert!(header.len() <= 8192);
        }
        assert!(header.starts_with(b"HTTP/1.1 200"));
        assert_eq!(session.next().await.unwrap().unwrap().data.as_ref(), b"abc");
        waiting.wait_for(|phase| *phase == 1).await.unwrap();
        session.life.close();
        waiting.wait_for(|phase| *phase == 2).await.unwrap();
        {
            let mut byte = [0];
            let read = client.read(&mut byte);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                use std::future::Future;
                assert!(read.as_mut().poll(cx).is_pending(), "response must await the original request EOF");
                Poll::Ready(())
            }).await;
        }
        client.write_all(if malformed { b"invalid\r\n" } else { b"0\r\n\r\n" }).await.unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        let read_result = client.read_to_end(&mut response).await;
        let result = server.await.unwrap();
        if malformed {
            assert!(result.is_err());
            assert!(session.life.error().is_err(), "actual HTTP body failure remains retained");
            assert!(!response.ends_with(b"0\r\n\r\n"));
        } else {
            read_result.unwrap();
            result.unwrap();
            session.life.error().unwrap();
            assert!(response.ends_with(b"0\r\n\r\n"));
        }
    }).await.unwrap();
}

#[tokio::test]
async fn logical_return_drains_actual_http1_request_before_response_eof() {
    actual_upload_return(false).await;
}

#[tokio::test]
async fn malformed_actual_upload_after_logical_return_remains_a_wire_error() {
    actual_upload_return(true).await;
}
