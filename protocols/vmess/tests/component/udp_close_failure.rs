use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Both faults come from the same real TCP original. The peer is dropped with
// unread accepted data, so the client observes reset before shutdown(Write).
#[cfg(unix)]
#[tokio::test]
async fn actual_reset_and_shutdown_sources_share_one_retained_terminal_record() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        client.write_all(&[1]).await.unwrap();
        let mut byte = [0; 1];
        assert_eq!(peer.peek(&mut byte).await.unwrap(), 1);
        drop(peer);
        let read = client.read(&mut byte).await.unwrap_err();
        assert_eq!(read.kind(), std::io::ErrorKind::ConnectionReset);
        let client = client.into_std().unwrap();
        let shutdown = client.shutdown(std::net::Shutdown::Write).unwrap_err();
        let failures = Failures::default();
        let error = |source| {
            ChannelError::new(
                Stage::Close,
                Reason::Io,
                zero_platform_tokio::contract::Impact::Session,
            )
            .with_source(source)
        };
        failures.read(error(read));
        let mut report = CloseReport::default();
        report.closed_io.push("same returned logical view".into());
        let first = failures.finish(&mut report).unwrap_err();
        assert_eq!(report.closed_io.len(), 1);
        let retained = Arc::new(first);
        let read = failures.sources.read.get().unwrap().clone();
        assert!(read.source().is_some());
        assert!(!failures.receipt.is_acknowledged());
        assert!(failures.receipt.acknowledge_retained(&retained));
        // The already retained record also owns a later independent direction
        // fault. No second receipt, native close proof or retry history exists.
        failures.shutdown(error(shutdown));
        let write = failures.sources.shutdown.get().unwrap().clone();
        assert!(write.source().is_some());
        assert!(Arc::ptr_eq(failures.sources.read.get().unwrap(), &read));
        let weak = Arc::downgrade(&failures.sources);
        let completed = failures.finish(&mut report).unwrap();
        assert_eq!(completed.closed_io, ["same returned logical view"]);
        assert!(report.closed_io.is_empty());
        drop(failures);
        drop(read);
        drop(write);
        assert!(
            weak.upgrade().is_some(),
            "real receiver still owns both sources"
        );
        drop(retained);
        assert!(weak.upgrade().is_none(), "no IO/Scope/controller cycle");
        drop(client);
    })
    .await
    .unwrap();
}
