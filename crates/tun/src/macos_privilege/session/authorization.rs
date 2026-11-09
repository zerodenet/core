//! Cancellation is a private authorization result, never PID/EOF inference.
use super::*;
impl native::Native {
    pub(super) fn prepare_authorization_output(&mut self, id: u64) -> io::Result<()> {
        let mut pair = [-1; 2];
        if unsafe { libc::pipe(pair.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        self.auth_ends = [
            Some(Original::new(pair[0], id)),
            Some(Original::new(pair[1], id)),
        ];
        for index in 0..2 {
            let original = self.auth_ends[index].as_mut().unwrap();
            let fd = original.fd();
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
            {
                return Err(io::Error::last_os_error());
            }
            if fd <= 2 {
                let next = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
                if next < 0 {
                    return Err(io::Error::last_os_error());
                }
                let slot = if index == 0 {
                    &mut self.auth_output
                } else {
                    &mut self.auth_writer
                };
                *slot = Some(Original::new(next, id)); // before closing/replacing old
                original.close_once()?;
            }
        }
        if self.auth_output.is_none() {
            self.auth_output = self.auth_ends[0].take();
        }
        if self.auth_writer.is_none() {
            self.auth_writer = self.auth_ends[1].take();
        }
        let read = self.auth_output.as_ref().unwrap().fd();
        let flags = unsafe { libc::fcntl(read, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(read, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn observe_authorization(&mut self, id: u64) -> io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if self.waited.is_none() {
            self.waited = child.try_wait()?;
        }
        let Some(status) = self.waited else {
            return Ok(());
        };
        if !status.success() {
            return Err(io::Error::other(
                "actual authorization Child exited before helper authentication",
            ));
        }
        let Some(output) = self.auth_output.as_mut() else {
            return Err(io::Error::other(
                "actual authorization output original is missing",
            ));
        };
        if !output.usable() {
            return Err(io::Error::other("authorization output close is unknown"));
        }
        loop {
            let mut byte = [0_u8];
            let count = unsafe { libc::read(output.fd(), byte.as_mut_ptr().cast(), 1) };
            if count == 0 {
                self.auth_eof = true;
                break;
            }
            if count < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if source.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                return Err(source);
            }
            if self.auth_read == self.auth_bytes.len() {
                return Err(io::Error::other(
                    "authorization result exceeds its fixed workspace",
                ));
            }
            self.auth_bytes[self.auth_read] = byte[0];
            self.auth_read += 1;
        }
        if self.auth_eof {
            // Only the fixed script's -128 branch emits this exact nonce. It
            // did not execute the privileged command, unlike a signal/EOF.
            if cancelled(&self.auth_bytes[..self.auth_read], id) {
                self.cancelled = true;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "administrator authorization was cancelled before helper execution",
                ));
            }
            return Err(io::Error::other(
                "authorization ended without the expected helper acquisition",
            ));
        }
        Ok(())
    }
    pub(super) fn close_authorization_output(&mut self) -> io::Result<()> {
        let mut failure = None;
        for original in [&mut self.auth_output, &mut self.auth_writer]
            .into_iter()
            .chain(self.auth_ends.iter_mut())
            .flatten()
        {
            if let Err(source) = original.close_once() {
                failure.get_or_insert(source);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

fn cancelled(bytes: &[u8], id: u64) -> bool {
    let Some(bytes) = bytes
        .strip_prefix(b"zero.utun.cancelled.")
        .and_then(|bytes| bytes.strip_suffix(b"\n"))
    else {
        return false;
    };
    !bytes.is_empty()
        && bytes.iter().all(u8::is_ascii_digit)
        && std::str::from_utf8(bytes).is_ok_and(|value| value.parse::<u64>() == Ok(id))
}
#[cfg(test)]
mod tests;
