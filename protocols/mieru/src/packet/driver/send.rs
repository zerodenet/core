use super::*;
impl Driver {
    pub(super) fn command(&mut self, command: Command) -> io::Result<()> {
        match command {
            Command::Data {
                id,
                payload,
                status,
            } => {
                if !status.closed() {
                    if let Some(state) = self.states.get_mut(&id) {
                        state.reliable.queue_data(&payload)?;
                    }
                }
            }
            Command::Barrier {
                id,
                close,
                status,
                done,
            } => {
                if let Some(state) = self.states.get_mut(&id).filter(|_| !status.closed()) {
                    state.barriers.push(Barrier {
                        through: state.reliable.next_send,
                        close,
                        done,
                    });
                } else {
                    let _ = done.send(Err(io::ErrorKind::BrokenPipe.into()));
                }
            }
            Command::Close { id, .. } => {
                if let Some(state) = self.states.get_mut(&id) {
                    if state.closing.is_none() {
                        state
                            .reliable
                            .queue_control(crate::metadata::CLOSE_SESSION_REQUEST)?;
                        state.closing = Some(Instant::now());
                    }
                }
            }
            Command::Retire(id) => self.retire(id, Retirement::Local),
            Command::Open(_) | Command::OpenClient(_) => {
                return Err(io::Error::other("unexpected packet open command"))
            }
        }
        Ok(())
    }
    pub(super) async fn flush(&mut self) -> io::Result<()> {
        let ids: Vec<_> = self.states.keys().copied().collect();
        for id in ids {
            self.drain(id).await?;
            let Some(state) = self.states.get_mut(&id) else {
                continue;
            };
            if state.reliable.last_rx.elapsed() > Duration::from_secs(60)
                || state
                    .closing
                    .is_some_and(|at| at.elapsed() > Duration::from_secs(15))
            {
                self.retire(id, Retirement::Failed(io::ErrorKind::TimedOut));
                continue;
            }
            state.advance_barriers()?;
            let capacity = self
                .book
                .sessions
                .get(&id)
                .map_or(0, |entry| entry.sender.capacity());
            match state.reliable.poll_transmissions(capacity) {
                Ok(packets) => {
                    for tx in packets {
                        self.io.send(&self.codec.encode(&tx.segment)?).await?;
                    }
                }
                Err(error) => self.retire(id, Retirement::Failed(error.kind())),
            }
        }
        self.retired
            .retain(|_, tomb| tomb.since.elapsed() < Duration::from_secs(360));
        Ok(())
    }
}
