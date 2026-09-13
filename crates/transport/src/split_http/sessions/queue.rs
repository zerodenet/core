use super::*;
mod pending;

impl Session {
    pub(in crate::split_http) async fn push(
        &self,
        sequence: u64,
        bytes: Bytes,
        streaming: bool,
    ) -> io::Result<()> {
        let budget = self.reserve_upload(bytes.len())?;
        self.push_reserved(sequence, bytes, streaming, budget).await
    }
    pub(in crate::split_http) fn reserve_upload(
        &self,
        size: usize,
    ) -> io::Result<OwnedSemaphorePermit> {
        if size > MAX_BYTES {
            return Err(io::Error::other("xhttp packet exceeds session byte limit"));
        }
        self.budget
            .clone()
            .try_acquire_many_owned(size as u32)
            .map_err(|_| io::Error::other("xhttp listener upload budget exhausted"))
    }
    pub(in crate::split_http) async fn push_reserved(
        &self,
        sequence: u64,
        bytes: Bytes,
        streaming: bool,
        budget: OwnedSemaphorePermit,
    ) -> io::Result<()> {
        let pending = pending::Pending::new(self, sequence, bytes, streaming, budget)?;
        // Xray holds writeCloseMutex while sending into its bounded channel.
        // Keep FIFO ownership across backpressure: waking all blocked HTTP
        // producers must not let later sequences overtake the oldest waiter.
        let writer = self.writer.lock();
        tokio::pin!(writer);
        let _writer = loop {
            let space = self.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            if pending.consumed() {
                return Ok(());
            }
            tokio::select! {
                writer = &mut writer => break writer,
                _ = &mut space => {},
                _ = self.life.cancelled() => return Err(io::Error::other("xhttp session closed")),
            }
        };
        loop {
            let space = self.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            self.life.error()?;
            if self.life.is_closed() {
                return Err(io::Error::other("xhttp session closed"));
            }
            {
                let mut state = self.state.lock().unwrap();
                if pending.consumed() {
                    return Ok(());
                }
                if state.eof
                    || streaming != state.streaming
                    || sequence < state.next
                    || state.packets.contains_key(&sequence)
                    || state.incoming.iter().any(|(seq, _)| *seq == sequence)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "duplicate or inconsistent xhttp upload",
                    ));
                }
                // Match the reference's bounded ingress channel. An in-order burst
                // waits for the consumer, independently of its reassembly heap.
                if state.incoming.len() < self.max_posts && state.bytes + pending.len() <= MAX_BYTES
                {
                    let packet = pending.take().expect("pending packet under session lock");
                    state.bytes += packet.data.len();
                    state.incoming.push_back((sequence, packet));
                    self.ready.notify_one();
                    return Ok(());
                }
            }
            tokio::select! {
                _ = &mut space => {},
                _ = self.life.cancelled() => return Err(io::Error::other("xhttp session closed")),
            }
        }
    }
    pub(in crate::split_http) fn finish(&self) {
        self.state.lock().unwrap().eof = true;
        self.ready.notify_one();
        self.space.notify_waiters();
    }
    pub(in crate::split_http) async fn next(&self) -> io::Result<Option<Queued>> {
        let mut reassembly_deadline = None;
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                let next = state.next;
                loop {
                    if let Some(bytes) = state.packets.remove(&next) {
                        state.bytes -= bytes.data.len();
                        state.next = next
                            .checked_add(1)
                            .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
                        self.space.notify_waiters();
                        return Ok(Some(bytes));
                    }
                    // Xray checks the heap after popping its minimum: allow N+1
                    // pending out-of-order packets before rejecting the next one.
                    // A sequence gap alone is not an overflow.
                    if state.packets.len() > self.max_posts + 1 {
                        // The missing packet may already be completely received,
                        // held by a backpressured HTTP writer. Do not mistake our
                        // executor's queue order for a missing network packet.
                        // Promote only that exact sequence, reusing its existing
                        // byte reservation; the reassembly bound does not grow.
                        let received = if let Some(index) =
                            state.incoming.iter().position(|(seq, _)| *seq == next)
                        {
                            let (_, packet) = state.incoming.remove(index).unwrap();
                            state.bytes -= packet.data.len();
                            Some(packet)
                        } else {
                            state
                                .waiting
                                .get(&next)
                                .and_then(|packet| packet.lock().unwrap().take())
                        };
                        if let Some(packet) = received {
                            state.next = next
                                .checked_add(1)
                                .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
                            self.space.notify_waiters();
                            return Ok(Some(packet));
                        }
                        // Capacity is backpressure, not proof of packet loss:
                        // another HTTP connection can still be ready to supply
                        // the missing sequence. Stop draining ingress here and
                        // retain the existing bounded buffers until that packet
                        // arrives or the same 30-second upload deadline expires.
                        reassembly_deadline.get_or_insert_with(|| {
                            tokio::time::Instant::now() + Duration::from_secs(30)
                        });
                        break;
                    }
                    let Some((sequence, packet)) = state.incoming.pop_front() else {
                        break;
                    };
                    state.packets.insert(sequence, packet);
                    self.space.notify_waiters();
                }
                if state.eof {
                    return Ok(None);
                }
            }
            tokio::select! {
                _ = &mut ready => {}
                _ = self.life.cancelled() => { self.life.error()?; return Ok(None); }
                _ = tokio::time::sleep_until(reassembly_deadline.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(300))) => {
                    return Err(io::Error::new(io::ErrorKind::TimedOut,
                        if reassembly_deadline.is_some() { "xhttp reassembly timed out waiting for missing upload" } else { "xhttp upload idle" }));
                }
            }
        }
    }
    pub(in crate::split_http) async fn push_stream(
        &self,
        sequence: u64,
        bytes: Bytes,
    ) -> io::Result<()> {
        self.push(sequence, bytes, true).await
    }
}
