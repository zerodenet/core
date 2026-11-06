use super::*;
use std::future::Future;
#[cfg(unix)]
use std::{
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
};
#[cfg(unix)]
fn fixture() -> (Arc<ParentInputOwner>, UnixStream, UnixStream, i32) {
    let (read, write) = UnixStream::pair().unwrap();
    let owner = ParentInputOwner::new();
    owner
        .start_with(|| native::duplicate(read.as_raw_fd()))
        .unwrap();
    let original = lock(&owner.shared.native).raw() as i32;
    (owner, read, write, original)
}
#[cfg(unix)]
#[tokio::test]
async fn actual_pending_parent_read_keeps_original_until_worker_and_scope_joins() {
    let (owner, mut borrowed, mut writer, original) = fixture();
    let mut waiting = Box::pin(owner.wait());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending()))
            .await
    );
    assert!(unsafe { libc::fcntl(original, libc::F_GETFD) } >= 0);
    drop(waiting); // Cancellation of this caller is not an original join.
    assert!(!lock(&owner.shared.worker).joined);
    assert!(!lock(&owner.shared.witness).as_ref().unwrap().is_joined());
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.exited_tasks.len(), 1);
    assert!(report.failed_tasks.is_empty());
    assert!(lock(&owner.shared.worker).joined);
    assert!(lock(&owner.shared.witness).as_ref().unwrap().is_joined());
    assert_eq!(unsafe { libc::fcntl(original, libc::F_GETFD) }, -1);
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
    writer.write_all(b"still borrowed").unwrap();
    let mut bytes = [0; 14];
    borrowed.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"still borrowed");
}
#[cfg(unix)]
#[tokio::test]
async fn actual_parent_eof_still_requires_same_original_close_after_scope_join() {
    let (owner, _borrowed, mut writer, original) = fixture();
    writer.write_all(b"parent alive").unwrap();
    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(2), owner.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(lock(&owner.shared.worker).joined);
    assert!(lock(&owner.shared.witness).as_ref().unwrap().is_joined());
    assert!(
        unsafe { libc::fcntl(original, libc::F_GETFD) } >= 0,
        "EOF is not NativeClosed"
    );
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.exited_tasks.len(), 1);
    assert_eq!(unsafe { libc::fcntl(original, libc::F_GETFD) }, -1);
}
