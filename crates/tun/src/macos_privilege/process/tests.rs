use super::*;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

#[test]
fn real_registered_process_exit_needs_post_registration_liveness_and_its_event_close() {
    // The actual child is kept at an IPC suspension until the event is installed.
    let (mut parent, child) = UnixStream::pair().unwrap();
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        unsafe {
            libc::close(parent.as_raw_fd());
            let mut byte = [0_u8; 1];
            libc::read(child.as_raw_fd(), byte.as_mut_ptr().cast(), 1);
            let live = 71_u64.to_be_bytes();
            libc::write(child.as_raw_fd(), live.as_ptr().cast(), live.len());
            libc::read(child.as_raw_fd(), byte.as_mut_ptr().cast(), 1);
            libc::_exit(0);
        }
    }
    drop(child);
    let mut exit = ProcessExit::new();
    exit.register(pid, 71).unwrap();
    assert!(exit.observe_exit().unwrap().is_none());
    parent.write_all(&[1]).unwrap();
    let mut live = [0; 8];
    parent.read_exact(&mut live).unwrap();
    exit.observe_live(pid, u64::from_be_bytes(live)).unwrap();
    assert!(exit.observe_exit().unwrap().is_none());
    parent.write_all(&[1]).unwrap();
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    let receipt = exit.observe_exit().unwrap().expect("matching NOTE_EXIT");
    assert!(receipt.matches(pid, 71));
    assert!(!exit.event_closed());
    exit.close_event().unwrap();
    assert!(exit.event_closed());
}
