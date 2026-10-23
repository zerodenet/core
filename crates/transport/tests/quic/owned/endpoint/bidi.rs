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
