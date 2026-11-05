use super::*;
use std::{
    future::{poll_fn, Future},
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task::Poll,
};

#[tokio::test]
async fn parent_join_does_not_release_its_nested_original_blocking_lookup() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut release = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut reader, _) = listener.accept().unwrap();
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(4).unwrap());
    let tasks = OwnedDnsQueryTasks::new_critical(&scope.registrar(), NonZeroUsize::new(3).unwrap());
    let (origin, caller) = oneshot::channel();
    let (parent_exit, parent_end) = oneshot::channel();
    let (entered, started) = oneshot::channel();
    let nested = tasks.clone();
    tasks
        .spawn(
            async move {
                origin
                    .send(OwnedDnsQueryTasks::current_witness().unwrap())
                    .unwrap();
                let child = nested.clone();
                nested
                    .spawn(
                        async move {
                            let result = child
                                .lookup_system_with(
                                    OwnedDnsQueryTasks::current_witness().unwrap(),
                                    CancellationToken::new(),
                                    Instant::now() + Duration::from_secs(5),
                                    move || {
                                        entered.send(()).unwrap();
                                        reader.read_exact(&mut [0])?;
                                        Ok(("127.0.0.1", 0)
                                            .to_socket_addrs()?
                                            .map(|_| zero_traits::IpAddress::V4([127, 0, 0, 1]))
                                            .collect())
                                    },
                                )
                                .await
                                .unwrap();
                            assert!(result.joined.result().is_ok());
                        },
                        CancellationToken::new(),
                    )
                    .unwrap();
                parent_end.await.unwrap();
            },
            CancellationToken::new(),
        )
        .unwrap();
    let original = caller.await.unwrap();
    started.await.unwrap();
    let blocking = tasks
        .0
        .state
        .lock()
        .unwrap()
        .records
        .iter()
        .find(|record| record.kind == TaskKind::Blocking)
        .unwrap()
        .registration
        .get()
        .unwrap()
        .as_ref()
        .unwrap()
        .witness
        .clone();
    parent_exit.send(()).unwrap();
    let joined = scope.next_exit().await.unwrap();
    assert_eq!(joined.id, original.task_id().unwrap());
    assert!(original.is_joined());
    tasks.reap();
    let mut cleanup =
        Box::pin(tasks.close_descendants(&original, Instant::now() + Duration::from_secs(5)));
    poll_fn(|cx| {
        assert!(cleanup.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        !blocking.is_joined(),
        "the original OS work is still suspended on real I/O"
    );
    release.write_all(&[1]).unwrap();
    loop {
        tokio::select! {biased;
            result=&mut cleanup => { result.unwrap(); break; },
            _=scope.next_exit()=>{},
        }
    }
    assert!(blocking.is_joined());
    drop(cleanup);
    let end = Instant::now() + Duration::from_secs(2);
    let (closed, report) = tokio::join!(tasks.close(end), scope.shutdown(end));
    closed.unwrap();
    assert!(report.is_complete());
}
