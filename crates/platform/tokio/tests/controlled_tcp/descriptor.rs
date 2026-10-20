#![cfg(target_os = "linux")]
use super::*;
use std::os::fd::{AsRawFd, RawFd};
fn identity(fd: RawFd) -> Option<(libc::dev_t, libc::ino_t)> {
    loop {
        let mut stat = std::mem::MaybeUninit::uninit();
        // SAFETY: fstat only reads kernel metadata. It never takes ownership,
        // reconstructs or closes even a concurrently reused numeric fd.
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == 0 {
            let stat = unsafe { stat.assume_init() };
            return Some((stat.st_dev, stat.st_ino));
        }
        let source = io::Error::last_os_error();
        if source.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        assert_eq!(source.raw_os_error(), Some(libc::EBADF));
        return None;
    }
}

#[tokio::test]
async fn controlled_tcp_close_removes_the_original_linux_socket_identity_while_io_still_exists() {
    timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (stream, peer) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let stream = stream.unwrap();
        let mut peer = peer.unwrap().0;
        let fd = stream.as_raw_fd();
        let original = identity(fd).unwrap();
        assert_ne!(original.1, 0);
        let (mut io, mut control) =
            TokioSocket::new(stream).into_controlled(CancellationToken::new());
        let report = control.wait_closed(Instant::now()).await.unwrap();
        assert!(!report.is_complete());
        assert_eq!(identity(fd), Some(original));
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        // Numeric fd reuse by another test is allowed; the original kernel
        // socket identity must be gone. Closing a cloned fd would leave it here.
        assert_ne!(identity(fd), Some(original));
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(io.write_all(b"late").await.is_err());
    })
    .await
    .unwrap();
}
