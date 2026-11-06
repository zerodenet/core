use super::*;
#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::net::UnixStream};
use std::{
    future::Future,
    io::{Read, Write},
};
#[cfg(unix)]
fn fixture() -> (Arc<ParentInputOwner>, UnixStream, UnixStream, usize) {
    let (read, write) = UnixStream::pair().unwrap();
    let owner = ParentInputOwner::new();
    owner
        .start_with(|| native::duplicate(read.as_raw_fd()))
        .unwrap();
    let original = lock(&owner.shared.native).raw();
    (owner, read, write, original)
}
#[tokio::test]
async fn actual_pending_parent_read_keeps_original_until_worker_and_scope_joins() {
    let (owner, mut borrowed, mut writer, original) = fixture();
    let mut waiting = Box::pin(owner.wait());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending()))
            .await
    );
    assert!(is_open(original));
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
    assert!(!is_open(original));
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
    writer.write_all(b"still borrowed").unwrap();
    let mut bytes = [0; 14];
    borrowed.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"still borrowed");
}
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
    assert!(is_open(original), "EOF is not NativeClosed");
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.exited_tasks.len(), 1);
    assert!(!is_open(original));
}

#[cfg(windows)]
fn fixture() -> (
    Arc<ParentInputOwner>,
    std::io::PipeReader,
    std::io::PipeWriter,
    usize,
) {
    use std::os::windows::io::AsRawHandle;
    let (read, write) = std::io::pipe().unwrap();
    let owner = ParentInputOwner::new();
    owner
        .start_with(|| native::duplicate(read.as_raw_handle() as usize))
        .unwrap();
    let original = lock(&owner.shared.native).raw();
    (owner, read, write, original)
}
#[cfg(unix)]
fn is_open(original: usize) -> bool {
    unsafe { libc::fcntl(original as i32, libc::F_GETFD) >= 0 }
}
#[cfg(windows)]
fn is_open(original: usize) -> bool {
    let mut flags = 0;
    unsafe { windows_sys::Win32::Foundation::GetHandleInformation(original as _, &mut flags) != 0 }
}
