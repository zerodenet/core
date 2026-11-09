//! Runs only in Command::pre_exec; no allocator, locks, logging or Rust IO.
use super::*;
pub(super) fn announce(fd: RawFd, nonce: u64) -> io::Result<()> {
    let mut report = [0_u8; 16];
    report[..8].copy_from_slice(&nonce.to_be_bytes());
    report[8..12].copy_from_slice(&unsafe { libc::getpid() }.to_be_bytes());
    write(fd, &report)?;
    loop {
        let mut message = [0; crate::macos_privilege::command::control::MESSAGE];
        let mut offset = 0;
        while offset < message.len() {
            let count = unsafe {
                libc::read(
                    fd,
                    message[offset..].as_mut_ptr().cast(),
                    message.len() - offset,
                )
            };
            if count == 0 {
                return Err(io::Error::from_raw_os_error(libc::EPIPE));
            }
            if count < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(source);
            }
            offset += count as usize;
        }
        let received = u64::from_be_bytes(message[1..].try_into().unwrap());
        // One late cancellation of the previous, truly completed command may
        // follow its terminal frame. It grants nothing to this new Child.
        if matches!(
            message[0],
            crate::macos_privilege::command::control::CANCEL
                | crate::macos_privilege::command::control::EXIT
        ) && received != nonce
        {
            continue;
        }
        if message[0] != crate::macos_privilege::command::control::ACK || received != nonce {
            return Err(io::Error::from_raw_os_error(libc::EPROTO));
        }
        break;
    }
    let ack = nonce.to_be_bytes();
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
