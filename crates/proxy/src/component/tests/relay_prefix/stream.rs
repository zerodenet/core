use super::*;
use crate::adapters::socks5::relay_tests::fixtures;
#[tokio::test]
async fn callback_stream_uses_actual_donor_loan_preserves_wire_halfclose_and_caller() {
    timeout(Duration::from_secs(20), async {
        for kind in Kind::all() {
            for business in [true, false] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let fixture = Fixture::new(listener.local_addr().unwrap(), kind, business).await;
                let peer = tokio::spawn(async move {
                    let (mut peer, _) = listener.accept().await.unwrap();
                    assert_eq!(
                        fixtures::request(&mut peer).await,
                        (Address::Domain("203.0.113.9".into()), 443)
                    );
                    fixtures::accept(&mut peer).await;
                    peer.write_all(b"pre").await.unwrap();
                    let mut bytes = Vec::new();
                    peer.read_to_end(&mut bytes).await.unwrap();
                    assert_eq!(&bytes, b"payload");
                    peer.write_all(b"tail").await.unwrap();
                    peer.shutdown().await.unwrap();
                });
                let mut call = fixture.call();
                call.deadline = Some(Instant::now() + Duration::from_secs(5));
                let mut io = fixture
                    .prefix
                    .open_stream(
                        &fixture.prefix.expected_binding,
                        fixture.prefix.expected_endpoint.clone(),
                        call,
                    )
                    .await
                    .unwrap();
                let originals = fixture.originals();
                assert_eq!(originals.len(), 1);
                assert!(originals[0].closed().is_none());
                let mut pre = [0; 3];
                io.read_exact(&mut pre).await.unwrap();
                assert_eq!(&pre, b"pre");
                io.write_all(b"payload").await.unwrap();
                io.shutdown().await.unwrap();
                let mut tail = Vec::new();
                io.read_to_end(&mut tail).await.unwrap();
                assert_eq!(&tail, b"tail");
                drop(io);
                peer.await.unwrap();
                fixture.finish(originals).await;
            }
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn callback_parent_and_additional_cancellation_interrupt_actual_pending_read() {
    timeout(Duration::from_secs(20), async {
        for parent in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let fixture = Fixture::new(listener.local_addr().unwrap(), Kind::Accepted, true).await;
            let peer = tokio::spawn(async move {
                let (mut peer, _) = listener.accept().await.unwrap();
                let _ = fixtures::request(&mut peer).await;
                fixtures::accept(&mut peer).await;
                let mut byte = [0];
                assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
            });
            let call = fixture.call();
            let extra = call.cancel.clone();
            let mut io = fixture
                .prefix
                .open_stream(
                    &fixture.prefix.expected_binding,
                    fixture.prefix.expected_endpoint.clone(),
                    call,
                )
                .await
                .unwrap();
            let originals = fixture.originals();
            {
                let mut byte = [0];
                let reading = io.read(&mut byte);
                tokio::pin!(reading);
                assert!(
                    futures_util::poll!(reading.as_mut()).is_pending(),
                    "real native read, no data from peer"
                );
                if parent {
                    fixture.prefix.context.cancel.cancel();
                } else {
                    extra.cancel();
                }
                assert_eq!(
                    reading.await.unwrap_err().kind(),
                    std::io::ErrorKind::Interrupted
                );
            }
            drop(io);
            peer.await.unwrap();
            fixture.finish(originals).await;
        }
    })
    .await
    .unwrap();
}
