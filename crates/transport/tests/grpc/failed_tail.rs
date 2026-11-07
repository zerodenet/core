//! A real peer GOAWAY cannot discard an already-decoded blocked hunk.
use super::*;
use crate::{grpc, profile::OwnedGrpcProfile};
use std::error::Error;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn actual_failed_connection_retains_decoded_tail_until_original_fifo_returns() {
    timeout(Duration::from_secs(8), async {
        for multi_mode in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            let expected: Vec<u8> = (0..65 * grpc::GRPC_MAX_PAYLOAD + 1024)
                .map(|index| (index % 251) as u8)
                .collect();
            let body = expected.clone();
            let (break_connection, broken) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                io.set_nodelay(true).unwrap();
                let mut connection = h2::server::handshake(io).await.unwrap();
                let (request, mut respond) = connection.accept().await.unwrap().unwrap();
                let response = Response::builder()
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "0")
                    .body(())
                    .unwrap();
                let mut sender = respond.send_response(response, false).unwrap();
                let hunk = grpc::encode_grpc_hunk(&body);
                let mut frame = grpc::grpc_frame_header(hunk.len()).to_vec();
                frame.extend_from_slice(&hunk);
                let sending = async {
                    let mut bytes = Bytes::from(frame);
                    while !bytes.is_empty() {
                        sender.reserve_capacity(bytes.len());
                        let capacity = futures_util::future::poll_fn(|cx| sender.poll_capacity(cx))
                            .await
                            .unwrap()
                            .unwrap();
                        if capacity > 0 {
                            let chunk = bytes.split_to(bytes.len().min(capacity));
                            sender.send_data(chunk, false).unwrap();
                        }
                    }
                    futures_util::future::poll_fn(|cx| sender.poll_send_drained(cx))
                        .await
                        .unwrap();
                    broken.await.unwrap();
                };
                tokio::pin!(sending);
                tokio::select! {
                    _ = &mut sending => {},
                    result = futures_util::future::poll_fn(|cx|connection.poll_closed(cx)) =>
                        panic!("actual peer connection ended before the held decoded hunk: {result:?}"),
                }
                // This is a real nonzero HTTP/2 GOAWAY on the same native
                // connection, not a fabricated worker result or close receipt.
                connection.abrupt_shutdown(h2::Reason::INTERNAL_ERROR);
                let _ = futures_util::future::poll_fn(|cx| connection.poll_closed(cx)).await;
                drop(request);
            });
            let raw = tokio::net::TcpStream::connect(address).await.unwrap();
            raw.set_nodelay(true).unwrap();
            let profile = OwnedGrpcProfile {
                service_names: vec!["failed/Tun".into()],
                multi_mode,
                ..Default::default()
            };
            let prepared = grpc::PreparedSingleClient::prepare(raw, &profile, "localhost", None)
                .await
                .unwrap();
            let (mut stream, driver) = prepared.into_parts();
            let task = tokio::spawn(driver);
            while stream.read_rx.len() != 64 {
                assert!(!task.is_finished(), "real driver ended before FIFO filled");
                tokio::task::yield_now().await;
            }
            // Decoding one complete hunk preceded all 64 enqueued chunks;
            // its remaining chunk and 1024-byte tail are held by that worker.
            break_connection.send(()).unwrap();
            peer.await.unwrap();
            AsyncWriteExt::write_all(&mut stream, b"observe original connection failure")
                .await
                .unwrap();
            let failure = AsyncWriteExt::flush(&mut stream).await.unwrap_err();
            assert!(original_h2(&failure)
                .is_some_and(|error| error.reason() == Some(h2::Reason::INTERNAL_ERROR)));
            assert!(
                !task.is_finished(),
                "actual driver failure cannot drop its still-blocked original read worker"
            );
            let mut received = Vec::new();
            let failure = stream.read_to_end(&mut received).await.unwrap_err();
            assert_eq!(received.len(), expected.len());
            assert!(received == expected, "decoded tail or byte ordering changed");
            assert!(original_h2(&failure)
                .is_some_and(|error| error.reason() == Some(h2::Reason::INTERNAL_ERROR)));
            drop(stream);
            let failure = task.await.unwrap().unwrap_err();
            assert!(original_h2(&failure)
                .is_some_and(|error| error.reason() == Some(h2::Reason::INTERNAL_ERROR)));
        }
    })
    .await
    .unwrap();
}

fn original_h2<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a h2::Error> {
    let mut pending = vec![error];
    let mut seen: Vec<&(dyn Error + 'static)> = Vec::new();
    while let Some(error) = pending.pop() {
        if seen.iter().any(|prior| std::ptr::eq(*prior, error)) {
            continue;
        }
        if seen.len() == 64 {
            break;
        }
        seen.push(error);
        if let Some(error) = error.downcast_ref::<h2::Error>() {
            return Some(error);
        }
        if let Some(inner) = error
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
        {
            pending.push(inner);
        }
        if let Some(inner) = error.source() {
            pending.push(inner);
        }
    }
    None
}
