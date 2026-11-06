use super::*;
use crate::contract::{
    lifecycle::ChannelControl,
    scope::{TaskClass, TaskScope},
};
use std::{
    future::Future,
    num::NonZeroUsize,
    os::fd::{FromRawFd, OwnedFd},
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
    time::Instant,
};

fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(5)
}
fn child_handle(pid: i32) -> OwnedFd {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    assert!(
        fd >= 0,
        "original test descendant pidfd: {}",
        io::Error::last_os_error()
    );
    unsafe { OwnedFd::from_raw_fd(fd) }
}
fn readable(fd: &OwnedFd) -> bool {
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    assert!(unsafe { libc::poll(&mut poll, 1, 0) } >= 0);
    poll.revents & libc::POLLIN != 0
}
async fn tree(natural: bool) {
    let wire = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = wire.local_addr().unwrap();
    let copied_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let copied_address = copied_listener.local_addr().unwrap();
    let (listener_view, mut listener_original) = crate::ControlledTcpListener::new(
        copied_listener,
        tokio_util::sync::CancellationToken::new(),
    );
    let script = format!(
        r#"
import os,socket,struct,time
r,w=os.pipe()
if os.fork()==0:
 os.close(r)
 os.setsid()
 if os.fork()!=0: os._exit(0)
 s=socket.create_connection(('127.0.0.1',{}))
 s.sendall(struct.pack('!I',os.getpid()))
 if s.recv(1)==b'x': os.write(w,b'x')
 s.recv(1)
 os._exit(0)
os.close(w)
if {}:
 os.read(r,1)
 os._exit(23)
while True: time.sleep(60)
"#,
        address.port(),
        if natural { "True" } else { "False" }
    );
    let mut request = ProcessRequest::new("python3");
    request.arguments = vec!["-c".into(), script.into()];
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let original = OwnedProcess::prepare(request, &scope.registrar()).unwrap();
    let (begin, begun) = oneshot::channel();
    let (ready, startup) = oneshot::channel();
    let (returned, body) = oneshot::channel();
    let actor_original = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let witness = begun.await.unwrap();
            actor_original.start(witness).unwrap();
            actor_original.wait_started(end()).await.unwrap();
            let _ = ready.send(());
            // Explicitly establish the actual native tree wait is Pending.
            let mut wait = Box::pin(actor_original.wait_tree_ended(end()));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await
            );
            let result = wait.await;
            if result.is_err() {
                let state = actor_original.lock();
                eprintln!("actual process wait failed: helper={:?} root={:?} helper_status={:?} root_status={:?} tree={} valid={} eof={} original_error={:?}", state.helper, state.root, state.helper_status, state.root_status, state.tree, state.conversation_valid, state.eof, state.first_error);
            }
            result.unwrap();
            let _ = returned.send(());
            Ok(())
        })
        .unwrap();
    begin.send(witness.clone()).unwrap();
    tokio::time::timeout_at(end(), startup)
        .await
        .unwrap()
        .unwrap();
    let (mut wire_peer, _) = tokio::time::timeout_at(end(), wire.accept())
        .await
        .unwrap()
        .unwrap();
    let pid = tokio::time::timeout_at(end(), wire_peer.read_u32())
        .await
        .unwrap()
        .unwrap() as i32;
    let descendant = child_handle(pid);
    assert!(
        !readable(&descendant),
        "actual detached descendant is still live"
    );
    assert!(original.try_finish().unwrap().is_none());
    assert!(original.closed().is_none());
    assert!(!witness.is_joined());
    // Native helper descriptor isolation is verified while both the job and
    // its detached descendant remain live, using the original close report.
    let closed_listener = listener_original.wait_closed(end()).await.unwrap();
    assert!(closed_listener.is_complete());
    assert_eq!(closed_listener.closed_io.len(), 1);
    let rebound = tokio::net::TcpListener::bind(copied_address).await.unwrap();
    assert!(!readable(&descendant));
    if natural {
        wire_peer.write_all(b"x").await.unwrap();
    } else {
        original.request_stop();
    }
    tokio::time::timeout_at(end(), body).await.unwrap().unwrap();
    assert!(
        readable(&descendant),
        "actual detached descendant native exit"
    );
    assert!(progress::physical(&original.lock()));
    assert!(
        original.try_finish().unwrap().is_none(),
        "physical tree ending cannot stand in for the actor join"
    );
    let exit = tokio::time::timeout_at(end(), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.result.is_ok());
    assert!(witness.is_joined());
    let receipt = original
        .try_finish()
        .unwrap()
        .expect("all exact original gates completed");
    assert!(receipt.matches(&original));
    assert!(
        original.first_failure().is_none(),
        "{:?}",
        original.first_failure()
    );
    if natural {
        assert_eq!(original.root_status().unwrap().code(), Some(23));
    } else {
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&original.root_status().unwrap()),
            Some(libc::SIGKILL)
        );
    }
    assert!(scope.shutdown(end()).await.is_complete());
    drop((listener_view, rebound));
}
#[tokio::test]
async fn actual_process_stop_drains_detached_tree_and_requires_same_actor_join_and_fd_closes() {
    tree(false).await;
}
#[tokio::test]
async fn actual_natural_root_exit_drains_detached_tree_preserves_status_and_isolates_core_fds() {
    tree(true).await;
}

#[tokio::test]
async fn actual_exec_failure_retains_original_error_until_tree_and_actor_cleanup() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let original = OwnedProcess::prepare(
        ProcessRequest::new("/zero-test-absent-native-tree-root"),
        &scope.registrar(),
    )
    .unwrap();
    let (send, receive) = oneshot::channel();
    let owner = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            owner.start(receive.await.unwrap()).unwrap();
            let error = owner.wait_started(end()).await.unwrap_err();
            assert!(std::error::Error::source(&error).is_some());
            assert_eq!(
                owner.first_failure().unwrap().raw_os_error(),
                Some(libc::ENOENT)
            );
            owner.request_stop();
            owner.wait_tree_ended(end()).await.unwrap();
            Ok(())
        })
        .unwrap();
    send.send(witness).unwrap();
    scope.next_exit().await.unwrap().result.unwrap();
    assert!(original.try_finish().unwrap().unwrap().matches(&original));
    assert_eq!(
        original.first_failure().unwrap().raw_os_error(),
        Some(libc::ENOENT)
    );
    assert!(scope.shutdown(end()).await.is_complete());
}
