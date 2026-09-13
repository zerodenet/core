use super::*;

/// Borrowed registration of a completely received, byte-budgeted upload.
/// Cancellation drops the registration and reservation; HTTP ownership transfer
/// retains this guard in the existing bounded admission task.
pub(super) struct Pending<'a> {
    session: &'a Session,
    sequence: u64,
    packet: Arc<Mutex<Option<Queued>>>,
}
impl<'a> Pending<'a> {
    pub(super) fn new(
        session: &'a Session,
        sequence: u64,
        bytes: Bytes,
        streaming: bool,
        budget: OwnedSemaphorePermit,
    ) -> io::Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(io::Error::other("xhttp packet exceeds session byte limit"));
        }
        let mut state = session.state.lock().unwrap();
        if state.eof
            || streaming != state.streaming
            || sequence < state.next
            || state.packets.contains_key(&sequence)
            || state.waiting.contains_key(&sequence)
            || state.incoming.iter().any(|(seq, _)| *seq == sequence)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "duplicate or inconsistent xhttp upload",
            ));
        }
        let packet = Arc::new(Mutex::new(Some(Queued {
            data: bytes,
            _budget: budget,
        })));
        state.waiting.insert(sequence, packet.clone());
        session.ready.notify_one();
        Ok(Self {
            session,
            sequence,
            packet,
        })
    }
    pub(super) fn consumed(&self) -> bool {
        self.packet.lock().unwrap().is_none()
    }
    pub(super) fn len(&self) -> usize {
        self.packet
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |p| p.data.len())
    }
    pub(super) fn take(&self) -> Option<Queued> {
        self.packet.lock().unwrap().take()
    }
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.session
            .state
            .lock()
            .unwrap()
            .waiting
            .remove(&self.sequence);
    }
}
