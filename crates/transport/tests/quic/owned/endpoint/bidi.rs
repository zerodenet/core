use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn actual_erased_bidi_stream_holds_connection_and_both_stream_users_until_original_drop() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(3));
        let (mut owner, mut receiver, handle, retained) = owner(&scope, 6);
        let local = handle.local_addr().unwrap();
        let (client, server) = tokio::join!(handle.connect(local, "localhost"), handle.accept());
        let server = server.unwrap();
        let stream = client.unwrap().into_bi_stream().await.unwrap();
        assert_eq!(stream.remote_address(), local);
        let mut stream = zero_platform_tokio::TcpRelayStream::new(stream);
        let ((), halves) = tokio::join!(
            async {
                stream.write_all(b"query").await.unwrap();
                stream.shutdown().await.unwrap();
            },
            server.accept_bi()
        );
        let (mut send, mut recv) = halves.unwrap();
        let mut query = [0; 5];
        recv.read_exact(&mut query).await.unwrap();
        assert_eq!(&query, b"query");
        assert_eq!(
            recv.read(&mut [0; 1]).await.unwrap(),
            0,
            "actual QUIC FIN survives erasure"
        );
        send.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        assert_eq!(owner.active_users(), 6);
        drop(send);
        drop(recv);
        drop(server);
        assert_eq!(
            owner.active_users(),
            3,
            "erased stream retains its real Connection and TX/RX users"
        );
        owner.begin_shutdown();
        assert!(retained.local_addr().is_ok());
        assert_eq!(
            owner.shutdown(Instant::now()).await.unwrap_err().reason,
            Reason::Timeout
        );
        drop(stream);
        assert_eq!(owner.active_users(), 0);
        let (report, _) = tokio::join!(
            owner.shutdown(Instant::now() + Duration::from_secs(3)),
            collect(&mut scope, &mut receiver, 3)
        );
        assert!(report.unwrap().is_complete());
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_finish_ack_survives_cancelled_waiter_and_keeps_full_reverse_tail_before_original_drop(
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(3));
        let (mut owner, mut receiver, handle, retained) = owner(&scope, 6);
        let local = handle.local_addr().unwrap();
        let (client, server) = tokio::join!(handle.connect(local, "localhost"), handle.accept());
        let mut client = client.unwrap().into_bi_stream().await.unwrap();
        let server = server.unwrap();
        let payload: Vec<u8> = (0..262144).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let write = async {
            client.write_all(&payload).await.unwrap();
            client.flush().await.unwrap();
            // This is a current-thread runtime: the freshly queued FIN cannot
            // have an ACK before the original Quinn driver next gets a poll.
            // Poll the real shutdown, then cancel only this borrowing waiter.
            {
                let shutdown = tokio::io::AsyncWriteExt::shutdown(&mut client);
                tokio::pin!(shutdown);
                std::future::poll_fn(|cx| {
                    assert!(
                        shutdown.as_mut().poll(cx).is_pending(),
                        "FIN enqueue is not its real delivery ACK"
                    );
                    Poll::Ready(())
                })
                .await;
            }
            assert!(
                retained.local_addr().is_ok(),
                "pending write-half shutdown did not close native IO"
            );
            tokio::io::AsyncWriteExt::shutdown(&mut client)
                .await
                .unwrap();
            let mut reverse = Vec::new();
            client.read_to_end(&mut reverse).await.unwrap();
            assert_eq!(reverse.len(), payload.len() + 1025);
            assert_eq!(&reverse[..payload.len()], &payload);
            assert_eq!(&reverse[payload.len()..], &[73; 1025]);
            drop(client);
        };
        let reply = async {
            let mut stream = server.accept_bi_stream().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, expected);
            stream.write_all(&received).await.unwrap();
            stream.write_all(&[73; 1025]).await.unwrap();
            stream.shutdown().await.unwrap();
            // Last server connection/view now drops only after the actual ACK.
            // No native-close/Scope proof is inferred from either direction EOF.
            drop(stream);
        };
        tokio::join!(write, reply);
        assert_eq!(owner.active_users(), 0);
        assert!(
            retained.local_addr().is_ok(),
            "logical return is not native closure"
        );
        owner.begin_shutdown();
        let (closed, classes) = tokio::join!(
            owner.shutdown(Instant::now() + Duration::from_secs(3)),
            collect(&mut scope, &mut receiver, 3)
        );
        assert!(closed.unwrap().is_complete());
        assert_eq!(classes.len(), 3);
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert!(scope.shutdown(Instant::now()).await.is_complete());
    })
    .await
    .unwrap();
}
