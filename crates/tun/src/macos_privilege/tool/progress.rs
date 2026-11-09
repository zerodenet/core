//! Short nonblocking progression; no Session outcome lock spans IPC waits.
use super::*;
impl Supervisor {
    pub(in crate::macos_privilege) fn advance(&mut self) -> io::Result<()> {
        let Some(nonce) = self.nonce else {
            return Ok(());
        };
        let fd = self
            .poll_fd()
            .ok_or_else(|| io::Error::other("tool supervisor original is unavailable"))?;
        while self.read < self.report.len() {
            let count = unsafe {
                libc::read(
                    fd,
                    self.report[self.read..].as_mut_ptr().cast(),
                    self.report.len() - self.read,
                )
            };
            match result(count)? {
                Some(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                Some(count) => self.read += count,
                None => return Ok(()),
            }
        }
        if u64::from_be_bytes(self.report[..8].try_into().unwrap()) != nonce
            || self.report[12..] != [0; 4]
        {
            return Err(io::Error::other(
                "tool child report belongs to another request",
            ));
        }
        let pid = i32::from_be_bytes(self.report[8..12].try_into().unwrap());
        if self.child.event_fd().is_none() {
            self.child.register(pid, nonce)?;
        }
        let ack = nonce.to_be_bytes();
        while self.ack < ack.len() {
            let count =
                unsafe { libc::write(fd, ack[self.ack..].as_ptr().cast(), ack.len() - self.ack) };
            match result(count)? {
                Some(0) => return Err(io::Error::from_raw_os_error(libc::EPIPE)),
                Some(count) => self.ack += count,
                None => return Ok(()),
            }
        }
        while self.live_read < self.live.len() {
            let count = unsafe {
                libc::read(
                    fd,
                    self.live[self.live_read..].as_mut_ptr().cast(),
                    self.live.len() - self.live_read,
                )
            };
            match result(count)? {
                Some(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                Some(count) => self.live_read += count,
                None => return Ok(()),
            }
        }
        if self.live != ack {
            return Err(io::Error::other(
                "tool LIVE does not match the registered process",
            ));
        }
        self.child.observe_live(pid, nonce)
    }
}
fn result(count: isize) -> io::Result<Option<usize>> {
    if count >= 0 {
        return Ok(Some(count as usize));
    }
    let source = io::Error::last_os_error();
    match source.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => Ok(None),
        _ => Err(source),
    }
}
