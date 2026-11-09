//! The original request and response offsets survive a failed observer/waiter.
use super::*;
use std::io::Cursor;
pub(super) const CAPACITY: usize = 65_536;
mod output;
mod request;
pub(super) use request::Request;
pub(super) struct Rpc {
    request: Request,
    length: usize,
    written: usize,
    #[cfg(test)]
    response: Option<Box<[u8]>>,
    scratch: [u8; 4096],
    result_storage: Option<
        [crate::packet_memory::PacketMetadataArc<super::super::command::inline::Storage>; 2],
    >,
    receiving: Option<super::super::command::receive::Receiving>,
    read: usize,
    newline: bool,
    started: bool,
    command: Option<u64>,
    command_limit: usize,
    parsed: Option<HelperResponse>,
    owned_response: Option<(
        crate::packet_memory::PacketMemoryView,
        crate::packet_memory::PacketMetadataVec<u8>,
        usize,
    )>,
}
impl Rpc {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        let mut rpc = Self::with_request(Request::library());
        rpc.response = Some(vec![0; CAPACITY].into_boxed_slice());
        rpc
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
            #[cfg(test)]
            response: None,
            scratch: [0; 4096],
            result_storage: None,
            receiving: None,
            read: 0,
            newline: false,
            started: false,
            command: None,
            command_limit: 0,
            parsed: None,
            owned_response: None,
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
        let receiving = match command {
            Some(nonce) => Some(super::super::command::receive::Receiving::new(
                nonce,
                self.result_storage.as_ref().ok_or_else(|| {
                    io::Error::other("owned command lacks same Source output storage")
                })?,
            )?),
            None => None,
        };
        let mut cursor = Cursor::new(&mut self.request[..]);
        serde_json::to_writer(&mut cursor, request).map_err(io::Error::other)?;
        let length = usize::try_from(cursor.position()).map_err(io::Error::other)?;
        if length == self.request.len() {
            return Err(io::Error::other("helper request leaves no framing byte"));
        }
        self.request[length] = b'\n';
        if let Some((_, bytes, _)) = &mut self.owned_response {
            bytes.retain(|_| false);
        }
        self.receiving = receiving;
        self.length = length + 1;
        self.written = 0;
        self.read = 0;
        self.newline = false;
        self.parsed = None;
        self.command = command;
        self.started = true;
        Ok(())
    }
    pub(super) fn bind_metadata(
        &mut self,
        memory: crate::packet_memory::PacketMemoryView,
        output_limit: usize,
    ) -> io::Result<()> {
        let max = output_limit
            .checked_mul(8)
            .and_then(|max| max.checked_add(1024))
            .ok_or_else(|| io::Error::other("helper response layout overflow"))?;
        self.command_limit = output_limit;
        self.owned_response = Some((memory, Default::default(), max));
        Ok(())
    }
    pub(super) fn replace_request(
        &mut self,
        bytes: usize,
        credit: crate::packet_memory::PacketMetadataCredit,
    ) -> io::Result<()> {
        if self.pending() {
            return Err(io::Error::other("cannot replace a submitted request"));
        }
        self.request = Request::from_prepaid(bytes, credit)?;
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
    pub(super) fn progress(
        &mut self,
        control: RawFd,
        tools: &mut tool::Supervisor,
    ) -> io::Result<bool> {
        if !self.started {
            return Ok(true);
        }
        if self.command.is_some() {
            if let Err(source) = tools.advance() {
                if self.receiving.is_none() || source.kind() != io::ErrorKind::UnexpectedEof {
                    return Err(source);
                }
            }
        }
        self.write_request(control)?;
        if !self.submitted() {
            return Ok(false);
        }
        if let Some(receiving) = &mut self.receiving {
            let (memory, _, _) = self
                .owned_response
                .as_ref()
                .ok_or_else(|| io::Error::other("owned command lacks original memory view"))?;
            // The JSON bootstrap bound is larger; the actual command bound is
            // stored separately and never inferred from serialized expansion.
            if !receiving.progress(control, &mut self.scratch, memory, self.command_limit)? {
                return Ok(false);
            }
            let report = receiving
                .finished()
                .ok_or_else(|| io::Error::other("missing exact command terminal frame"))?;
            return tools.completed_report(
                self.command.unwrap(),
                report.child_pid,
                report.operation_errno,
            );
        }
        while !self.newline {
            // Admit actual response capacity BEFORE dequeueing its next byte.
            // A failure keeps every submitted prefix and current original.
            if let Some((memory, bytes, max)) = &mut self.owned_response {
                bytes
                    .try_reserve_one(memory, *max)
                    .map_err(io::Error::other)?;
            }
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
            if let Some((_, bytes, _)) = &mut self.owned_response {
                bytes
                    .push_reserved(byte)
                    .map_err(|_| io::Error::other("prepaid helper response slot missing"))?;
            } else {
                #[cfg(test)]
                {
                    let response = self
                        .response
                        .as_mut()
                        .ok_or_else(|| io::Error::other("Kernel reply lacks owned storage"))?;
                    if self.read == response.len() {
                        return Err(io::Error::other(
                            "library helper response exceeds fixture storage",
                        ));
                    }
                    response[self.read] = byte;
                }
                #[cfg(not(test))]
                return Err(io::Error::other("Kernel reply lacks owned storage"));
            }
            self.read += 1;
        }
        if self.parsed.is_none() {
            let bytes = if let Some((_, bytes, _)) = &self.owned_response {
                &bytes[..]
            } else {
                #[cfg(test)]
                {
                    &self
                        .response
                        .as_ref()
                        .ok_or_else(|| io::Error::other("Kernel reply lacks owned storage"))?
                        [..self.read]
                }
                #[cfg(not(test))]
                {
                    return Err(io::Error::other("Kernel reply lacks owned storage"));
                }
            };
            self.parsed = Some(serde_json::from_slice(bytes).map_err(io::Error::other)?);
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
        self.receiving = None;
        self.started = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
