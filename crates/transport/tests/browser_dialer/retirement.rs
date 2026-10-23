use super::*;
use std::{io, pin::Pin};
use tokio::io::{AsyncRead, ReadBuf};

#[tokio::test]
async fn retired_browser_io_is_terminal_and_closes_its_owned_control_channel() {
    let server = server().await;
    let dialer = server.dialer();
    let mut agent = browser(&server).await;
    let peer = tokio::spawn(async move {
        assert_eq!(task(&mut agent).await["method"], "GET");
        agent.send(Message::Text("ok".into())).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), agent.next())
            .await
            .expect("retired control channel must close after its stream is released");
        assert!(result.is_none() || result.unwrap().is_err());
    });
    let mut stream = dialer
        .dial_get("https://relay.example/down", &http::HeaderMap::new())
        .await
        .unwrap();
    dialer.close();
    // Poll directly so the old permanently-Interrupted implementation fails
    // promptly even on Tokio versions whose read utilities retry in one poll.
    // No test or production timeout needs to be relaxed to diagnose the error.
    for _ in 0..2 {
        let mut bytes = [0; 1];
        let mut buffer = ReadBuf::new(&mut bytes);
        let error = std::future::poll_fn(|cx| Pin::new(&mut stream).poll_read(cx, &mut buffer))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }
    assert_eq!(
        stream.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
    assert_eq!(
        stream
            .write_all(b"must not submit")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::ConnectionAborted
    );
    assert_eq!(
        stream.flush().await.unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
    assert_eq!(
        stream.shutdown().await.unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
    drop(stream);
    peer.await.unwrap();
    server.shutdown().await;
}
