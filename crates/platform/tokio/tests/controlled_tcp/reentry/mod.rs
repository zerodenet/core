use super::*;
use crate::contract::lifecycle::ChannelControl;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::Waker,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration, Instant},
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(4)
}
async fn pair() -> (TokioSocket, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (io, peer) = tokio::join!(
        TokioSocket::connect_addr(listener.local_addr().unwrap()),
        listener.accept()
    );
    (io.unwrap(), peer.unwrap().0)
}
fn close_once(shared: &Arc<Shared>) -> crate::contract::lifecycle::CloseReport {
    let mut control = ControlledTcpControl {
        shared: shared.clone(),
        reported: false,
    };
    let mut closing = Box::pin(control.wait_closed(end()));
    match closing
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(Ok(report)) => report,
        other => panic!("synchronous original close observation: {other:?}"),
    }
}
fn assert_unlocked(shared: &Shared) {
    drop(
        shared
            .state
            .try_lock()
            .expect("external callback must not hold original state"),
    );
    drop(
        shared
            .observer
            .try_lock()
            .expect("external callback must not hold observer slot"),
    );
}
mod close;
mod observer;
mod wake;
