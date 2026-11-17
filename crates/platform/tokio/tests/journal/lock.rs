//! A real fork retains the inherited lock description until explicitly reaped.
use super::*;
use std::os::fd::AsRawFd;

struct Child(libc::pid_t);
impl Drop for Child {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.0, libc::SIGKILL);
            libc::waitpid(self.0, std::ptr::null_mut(), 0);
        }
    }
}
#[test]
fn positive_journal_close_releases_lock_while_forked_child_retains_description() {
    const MARKER: &str = "ZERO_JOURNAL_FORK_LOCK_CHILD";
    if std::env::var_os(MARKER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "journal::tests::lock::positive_journal_close_releases_lock_while_forked_child_retains_description", "--nocapture"])
            .env(MARKER, "1").output().unwrap();
        assert!(String::from_utf8_lossy(&output.stdout).contains("running 1 test"));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let config = spec(dir.path().join("state"), 8);
    let owner = JournalOwner::standalone(config.clone()).unwrap();
    owner.start().unwrap();
    let (mut release, waiting) = std::os::unix::net::UnixStream::pair().unwrap();
    let fd = waiting.as_raw_fd();
    // No Rust allocation, locking or destruction in the forked branch; only
    // async-signal-safe libc calls until _exit. Run in an isolated test process.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        let mut byte = 0u8;
        unsafe {
            libc::read(fd, (&mut byte as *mut u8).cast(), 1);
            libc::_exit(0);
        }
    }
    let child = Child(pid);
    assert_eq!(
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
        0
    );
    let closed = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&closed));
    let next = JournalOwner::standalone(config).unwrap();
    next.start()
        .expect("positive original close releases this actual journal lock");
    assert!(next.matches_closed(&next.try_close().unwrap().unwrap()));
    use std::io::Write;
    release.write_all(&[1]).unwrap();
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    std::mem::forget(child);
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
}
