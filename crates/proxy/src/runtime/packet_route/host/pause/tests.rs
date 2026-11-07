//! Real native read Pending with the one original Scope registration.
//! This observes the private checkpoint; it does not qualify a native TUN.
use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
#[tokio::test]
async fn one_real_driver_checkpoint_seals_new_opens_and_resumes_the_same_pending_read_twice() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connecting = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
    let (client, server) = tokio::join!(connecting, listener.accept());
    let mut reader = client.unwrap();
    let mut writer = server.unwrap().0;
    let mut tasks = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (pause, checkpoint) = ReadPause::new(tasks.registrar().scope_identity());
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (_,actual)=tasks.registrar().spawn_tracked(TaskClass::Critical,async move {
        let mut bytes=[0;1];let read=reader.read(&mut bytes);tokio::pin!(read);
        std::future::poll_fn(|cx|{assert!(read.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        entered.send(()).unwrap();
        loop {tokio::select! {
            request=checkpoint.requested()=>{checkpoint.acknowledge(request,Ok(()));checkpoint.resumed(request).await;},
            read=&mut read=>{assert_eq!(read.unwrap(),1);break;}
        }}
        Ok(())
    }).unwrap();
    pause.bind(&actual).unwrap();
    pending.await.unwrap();
    let acquisition = pause.try_enter().unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut pausing = Box::pin(pause.fence(deadline));
    std::future::poll_fn(|cx| {
        assert!(pausing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(pause.try_enter().is_err());
    assert!(pause.state.lock().unwrap().result.is_none());
    assert!(!actual.is_joined());
    drop(acquisition);
    let fenced = pausing.await.unwrap();
    assert!(pause.try_enter().is_err());
    {
        let state = pause.state.lock().unwrap();
        assert_eq!(state.acquiring, 0);
        assert!(!state.armed && state.result.is_none());
    }
    assert!(!actual.is_joined());
    let paused = fenced.finish(deadline).await.unwrap();
    paused.validate().unwrap();
    assert!(!actual.is_joined());
    drop(paused);
    // Dropping the first phase restores only admission. No wire checkpoint,
    // join, native receipt or completed census is manufactured by the fence.
    let fenced = pause
        .fence(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(pause.try_enter().is_err());
    assert!(!pause.state.lock().unwrap().armed);
    assert!(pause.state.lock().unwrap().result.is_none());
    drop(fenced);
    drop(pause.try_enter().unwrap());
    assert!(!actual.is_joined());
    let paused = pause
        .pause(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    paused.validate().unwrap();
    assert!(!actual.is_joined());
    drop(paused);
    writer.write_all(&[9]).await.unwrap();
    tasks.next_exit().await.unwrap();
    assert!(actual.is_joined());
    assert!(pause.try_enter().is_err());
}
