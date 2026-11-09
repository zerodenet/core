//! Cancellation is a private authorization result, never PID/EOF inference.
use super::*;
impl native::Native {
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
        if let Some(output) = self.auth_output.as_mut() {
            output.close_once()?;
        }
        Ok(())
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
