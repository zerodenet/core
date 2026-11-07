#![cfg(windows)]
use std::{
    io,
    num::NonZeroUsize,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{contract::scope::TaskScope, ControlledPipeOwner};
static NEXT: AtomicUsize = AtomicUsize::new(0);
fn path() -> PathBuf {
    PathBuf::from(format!(
        r"\\.\pipe\zero-original-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
fn scope() -> TaskScope {
    TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    )
}
async fn pending(owner: &ControlledPipeOwner, which: usize) {
    tokio::time::timeout_at(end(), async {
        loop {
            let actual = owner.pending_io();
            if [actual.0, actual.1, actual.2][which] {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual original Win32 call must return ERROR_IO_PENDING");
}
#[tokio::test]
async fn cold_pipe_owner_opens_no_handle_or_worker() {
    let scope = scope();
    let owner = ControlledPipeOwner::new(path(), scope.registrar());
    assert!(owner.worker().is_none());
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed) && !closed.had_native_acquisition());
}
#[tokio::test]
async fn actual_pending_connect_cancel_joins_original_worker_before_native_close() {
    let mut scope = scope();
    let owner = ControlledPipeOwner::new(path(), scope.registrar());
    let pipe = owner.open().await.unwrap();
    pending(&owner, 0).await;
    {
        let connecting = pipe.connect();
        tokio::pin!(connecting);
        tokio::select! {biased;result=&mut connecting=>panic!("actual connect must remain pending: {result:?}"),_=tokio::task::yield_now()=>{}}
    }
    let witness = owner.worker().unwrap();
    assert!(!witness.is_joined());
    drop(pipe);
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(owner.std_worker_joined() && witness.is_joined());
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed) && closed.had_native_acquisition());
}
#[tokio::test]
async fn returned_worker_and_scope_cannot_close_a_still_held_unique_pipe_view() {
    let mut scope = scope();
    let owner = ControlledPipeOwner::new(path(), scope.registrar());
    let pipe = owner.open().await.unwrap();
    pending(&owner, 0).await;
    owner.request_stop();
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(owner.std_worker_joined() && owner.worker().unwrap().is_joined());
    assert!(!owner.view_returned());
    let error = match owner.close(tokio::time::Instant::now()).await {
        Err(error) => error,
        Ok(_) => panic!("held unique view must block original close"),
    };
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(owner.closed().is_none());
    drop(pipe);
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed));
    let other = ControlledPipeOwner::new(path(), scope.registrar());
    assert!(!other.matches_closed(&closed));
}
#[tokio::test]
async fn actual_pipe_bidirectional_io_and_pending_read_cancel_keep_separate_join_and_close_facts() {
    let mut scope = scope();
    let name = path();
    let owner = ControlledPipeOwner::new(name.clone(), scope.registrar());
    let mut server = owner.open().await.unwrap();
    let mut client = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(&name)
        .unwrap();
    server.connect().await.unwrap();
    pending(&owner, 1).await;
    client.write_all(b"actual-request").await.unwrap();
    let mut request = [0; 14];
    server.read_exact(&mut request).await.unwrap();
    assert_eq!(&request, b"actual-request");
    server.write_all(b"actual-response").await.unwrap();
    server.flush().await.unwrap();
    let mut response = [0; 15];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"actual-response");
    pending(&owner, 1).await;
    {
        let reading = server.read_u8();
        tokio::pin!(reading);
        tokio::select! {biased;result=&mut reading=>panic!("read must remain pending: {result:?}"),_=tokio::task::yield_now()=>{}}
    }
    assert!(owner.closed().is_none());
    drop(server);
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(owner.std_worker_joined());
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed) && closed.had_native_acquisition());
    drop(client);
}
#[tokio::test]
async fn cancelled_actual_pending_pipe_write_is_completed_before_the_original_buffers_and_handle_retire(
) {
    let mut scope = scope();
    let name = path();
    let owner = ControlledPipeOwner::new(name.clone(), scope.registrar());
    let mut server = owner.open().await.unwrap();
    let client = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(&name)
        .unwrap();
    server.connect().await.unwrap();
    let bytes = vec![7; 4 * 1024 * 1024];
    {
        let writing = server.write_all(&bytes);
        tokio::pin!(writing);
        tokio::select! {biased;result=&mut writing=>panic!("bounded unread peer must hold an actual pending write: {result:?}"),_=pending(&owner,2)=>{}}
    }
    assert!(owner.pending_io().2 && owner.closed().is_none());
    owner.request_stop();
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(owner.std_worker_joined());
    assert_eq!(
        server.flush().await.unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    drop(server);
    assert_eq!(owner.pending_io(), (false, false, false));
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed) && closed.had_native_acquisition());
    drop(client);
}
#[tokio::test]
async fn failed_native_pipe_creation_keeps_the_actual_worker_and_partial_event_originals_until_cleanup(
) {
    let mut scope = scope();
    let owner = ControlledPipeOwner::new(PathBuf::from("not-a-pipe"), scope.registrar());
    let error = match owner.open().await {
        Err(error) => error,
        Ok(_) => panic!("invalid original pipe namespace must fail"),
    };
    assert!(error.get_ref().is_some());
    let witness = owner.worker().unwrap();
    let _report = scope.shutdown(end()).await;
    assert!(witness.is_joined() && owner.std_worker_joined());
    assert!(witness.try_joined().unwrap().result().is_err());
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed) && !closed.had_native_acquisition());
}
