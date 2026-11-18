//! Retain real peer reads across another logical caller's return.
use std::{future::Future, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, watch},
};

pub(super) async fn start(
    length: usize,
    mut release: watch::Receiver<bool>,
) -> (u16, tokio::task::JoinHandle<()>, oneshot::Receiver<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready, pending) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await.unwrap();
        {
            let mut extra = [0];
            let read = stream.read(&mut extra);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                assert!(
                    read.as_mut().poll(cx).is_pending(),
                    "actual TCP peer must still await I/O"
                );
                Poll::Ready(())
            })
            .await;
            ready.send(()).unwrap();
            tokio::select! {biased;
                result = &mut read => panic!("UDP return revoked a live TCP original: {result:?}"),
                changed = release.changed() => changed.unwrap(),
            }
            assert!(*release.borrow());
        }
        stream.write_all(&bytes).await.unwrap();
        stream.shutdown().await.unwrap();
    });
    (port, task, pending)
}
