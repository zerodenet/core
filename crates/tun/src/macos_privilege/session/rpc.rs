//! The original request and response offsets survive a failed observer/waiter.
use super::*;
use std::io::Cursor;
pub(super) const CAPACITY: usize = 65_536;
mod request;
pub(super) use request::Request;
pub(super) struct Rpc {
    request: Request,
    length: usize,
    written: usize,
    response: [u8; CAPACITY],
    read: usize,
    newline: bool,
    started: bool,
    command: Option<u64>,
    parsed: Option<HelperResponse>,
}
impl Rpc {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self::with_request(Request::library())
    }
    pub(super) fn from_prepaid(
        bytes: usize,
        credit: crate::packet_memory::PacketMetadataCredit,
    ) -> io::Result<Self> {
        Ok(Self::with_request(Request::from_prepaid(bytes, credit)?))
    }
    fn with_request(request: Request) -> Self {
        Self {
            request,
            length: 0,
            written: 0,
            response: [0; CAPACITY],
            read: 0,
            newline: false,
            started: false,
            command: None,
            parsed: None,
        }
    }
    pub(super) fn begin(
        &mut self,
        request: &HelperRequest,
        command: Option<u64>,
    ) -> io::Result<()> {
        if self.started {
            return Err(io::Error::other(
                "same helper RPC still owns an unfinished request",
            ));
        }
        let mut cursor = Cursor::new(&mut self.request[..]);
        serde_json::to_writer(&mut cursor, request).map_err(io::Error::other)?;
        let length = usize::try_from(cursor.position()).map_err(io::Error::other)?;
        if length == self.request.len() {
            return Err(io::Error::other("helper request leaves no framing byte"));
        }
        self.request[length] = b'\n';
        self.length = length + 1;
        self.written = 0;
        self.read = 0;
        self.newline = false;
        self.parsed = None;
        self.command = command;
        self.started = true;
        Ok(())
    }
    pub(super) fn request_capacity(&self) -> usize {
        self.request.len()
    }
    pub(super) fn submitted(&self) -> bool {
        self.started && self.written == self.length
    }
    pub(super) fn pending(&self) -> bool {
        self.started
    }
    pub(super) fn command(&self) -> Option<u64> {
        self.command
    }
    pub(super) fn progress(
        &mut self,
        control: RawFd,
        tools: &mut tool::Supervisor,
    ) -> io::Result<bool> {
        if !self.started {
            return Ok(true);
        }
        if self.command.is_some() {
            tools.advance()?;
        }
        self.write_request(control)?;
        if !self.submitted() {
            return Ok(false);
        }
        while !self.newline {
            let mut byte = 0_u8;
            let count = unsafe { libc::read(control, (&mut byte as *mut u8).cast(), 1) };
            if count < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if source.kind() == io::ErrorKind::WouldBlock {
                    return Ok(false);
                }
                return Err(source);
            }
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            if byte == b'\n' {
                self.newline = true;
                break;
            }
            if self.read == self.response.len() {
                return Err(io::Error::other(
                    "helper response exceeds its retained RPC workspace",
                ));
            }
            self.response[self.read] = byte;
            self.read += 1;
        }
        if self.parsed.is_none() {
            self.parsed = Some(
                serde_json::from_slice(&self.response[..self.read]).map_err(io::Error::other)?,
            );
        }
        // Parsing a reply is independent of observing the exact child exit.
        // In particular, a helper exit never sets this condition for its tool.
        if self.command.is_some() && !tools.completed()? {
            return Ok(false);
        }
        Ok(true)
    }
    pub(super) fn write_request(&mut self, control: RawFd) -> io::Result<()> {
        while self.written < self.length {
            let bytes = &self.request[self.written..self.length];
            let count = unsafe { libc::write(control, bytes.as_ptr().cast(), bytes.len()) };
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
            if count == 0 {
                return Err(io::Error::from_raw_os_error(libc::EPIPE));
            }
            self.written += count as usize;
        }
        Ok(())
    }
    pub(super) fn take_response(&mut self) -> io::Result<HelperResponse> {
        let response = self
            .parsed
            .take()
            .ok_or_else(|| io::Error::other("same RPC reply is incomplete"))?;
        self.started = false;
        Ok(response)
    }
    /// Only the exact helper's positive exit may discard a known unsubmitted
    /// framing prefix. An admitted command remains owned by its tool event.
    pub(super) fn finish_after_helper_exit(
        &mut self,
        tools: &mut tool::Supervisor,
    ) -> io::Result<()> {
        if !self.started {
            return Ok(());
        }
        if let Some(nonce) = self.command {
            if !self.submitted() {
                tools.cancel_unsubmitted(nonce)?;
            } else if !tools.completed()? {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }
        self.started = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
