use super::*;
use std::{
    future::{poll_fn, Future},
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task::Poll,
};

#[tokio::test]
async fn network_fence_waits_for_the_original_blocking_child_after_its_query_is_cancelled() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut release = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut reader, _) = listener.accept().unwrap();
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(3).unwrap());
    let tasks = OwnedDnsQueryTasks::new_critical(&scope.registrar(), NonZeroUsize::new(2).unwrap());
    let old = tasks.clone();
    let (entered, started) = oneshot::channel();
    tasks
        .spawn_in_network(
            async move {
                let _ = old
                    .lookup_system_with(
                        OwnedDnsQueryTasks::current_witness().unwrap(),
                        OwnedDnsQueryTasks::current_cancellation().unwrap(),
                        Instant::now() + Duration::from_secs(5),
                        move || {
                            let _ = entered.send(());
                            reader.read_exact(&mut [0u8; 1])?;
                            Ok(("127.0.0.1", 0)
                                .to_socket_addrs()?
                                .map(|ip| match ip.ip() {
                                    std::net::IpAddr::V4(ip) => {
                                        zero_traits::IpAddress::V4(ip.octets())
                                    }
                                    std::net::IpAddr::V6(ip) => {
                                        zero_traits::IpAddress::V6(ip.octets())
                                    }
                                })
                                .collect())
                        },
                    )
                    .await;
            },
            CancellationToken::new(),
            Some(1),
        )
        .unwrap();
    started.await.unwrap();
    let original = {
        let state = tasks.0.state.lock().unwrap();
        state
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
            .clone()
    };
    let mut wait = Box::pin(original.wait());
    poll_fn(|cx| {
        assert!(wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(wait);
    let next = tasks.clone();
    let (fenced, mut answer) = oneshot::channel();
    tasks
        .spawn_in_network(
            async move {
                let result = next
                    .fence_network(
                        2,
                        &OwnedDnsQueryTasks::current_witness().unwrap(),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await;
                let _ = fenced.send(result);
            },
            CancellationToken::new(),
            Some(2),
        )
        .unwrap();
    // The parent is really joined by the actual Scope. The original blocking
    // child remains suspended and must still fence new-network acquisition.
    let exit = scope.next_exit().await.unwrap();
    assert_ne!(exit.id, original.task_id().unwrap());
    assert!(!original.is_joined());
    assert!(matches!(
        answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.write_all(&[1]).unwrap();
    loop {
        tokio::select! {biased;
            result = &mut answer => { result.unwrap().unwrap(); break; },
            _ = scope.next_exit() => {},
        }
    }
    assert!(original.is_joined());
    let (closed, report) = tokio::join!(
        tasks.close(Instant::now() + Duration::from_secs(2)),
        scope.shutdown(Instant::now() + Duration::from_secs(2))
    );
    closed.unwrap();
    assert!(report.is_complete());
}
