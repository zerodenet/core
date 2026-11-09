//! Runs only in Command::pre_exec; no allocator, locks, logging or Rust IO.
use super::*;
pub(super) fn announce(fd: RawFd, nonce: u64) -> io::Result<()> {
    let mut report = [0_u8; 16];
    report[..8].copy_from_slice(&nonce.to_be_bytes());
    report[8..12].copy_from_slice(&unsafe { libc::getpid() }.to_be_bytes());
    write(fd, &report)?;
    let mut ack = [0; 8];
    let mut offset = 0;
    while offset < ack.len() {
        let count =
            unsafe { libc::read(fd, ack[offset..].as_mut_ptr().cast(), ack.len() - offset) };
        if count == 0 {
            return Err(io::Error::from_raw_os_error(libc::EPIPE));
        }
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        offset += count as usize;
    }
    if ack != nonce.to_be_bytes() {
        return Err(io::Error::from_raw_os_error(libc::EPROTO));
    }
    write(fd, &ack)
}
fn write(fd: RawFd, bytes: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let count =
            unsafe { libc::write(fd, bytes[offset..].as_ptr().cast(), bytes.len() - offset) };
        if count == 0 {
            return Err(io::Error::from_raw_os_error(libc::EPIPE));
        }
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        offset += count as usize;
    }
    Ok(())
}
