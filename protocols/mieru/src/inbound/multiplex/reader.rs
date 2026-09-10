//! Connection-level frame decoding and bounded session dispatch.
mod backlog;
#[cfg(test)]
mod tests;

use super::{
    stream::{MieruLogicalStream, Status},
    writer::Command,
};
use crate::{
    crypto::MieruCipher,
    metadata::*,
    segment::{parse_segment, Segment},
};
use backlog::Backlog;
use mieru_config::MieruReceivePolicy;
use std::{collections::BTreeMap, io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
    time::Instant,
};

pub(crate) const MAX_SESSIONS: usize = 256;
const RECEIVE_QUEUE: usize = 8;
pub(crate) struct Entry {
    pub(crate) status: Arc<Status>,
    pub(crate) sender: mpsc::Sender<Vec<u8>>,
    backlog: Backlog,
}
impl Drop for Entry {
    fn drop(&mut self) {
        self.status.close(Some(io::ErrorKind::ConnectionReset));
    }
}
pub(crate) struct Reader {
    pub(crate) sessions: BTreeMap<u32, Entry>,
    pub(crate) ready: mpsc::Sender<MieruLogicalStream>,
    pub(crate) outgoing: mpsc::Sender<Command>,
    pub(crate) dropped: mpsc::UnboundedSender<(u32, Arc<Status>)>,
    policy: MieruReceivePolicy,
    pending_bytes: usize,
}
impl Reader {
    pub(crate) fn new(
        ready: mpsc::Sender<MieruLogicalStream>,
        outgoing: mpsc::Sender<Command>,
        dropped: mpsc::UnboundedSender<(u32, Arc<Status>)>,
        policy: MieruReceivePolicy,
    ) -> Self {
        Self {
            sessions: BTreeMap::new(),
            ready,
            outgoing,
            dropped,
            policy,
            pending_bytes: 0,
        }
    }

    pub(crate) fn insert(&mut self, id: u32, payload: Vec<u8>) -> io::Result<()> {
        let stream = self.create(id, payload)?;
        match self.ready.try_send(stream) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.remove_session(id);
                Err(io::ErrorKind::WouldBlock.into())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.remove_session(id);
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "mieru session receiver closed",
                ))
            }
        }
    }
    pub(crate) fn create(&mut self, id: u32, payload: Vec<u8>) -> io::Result<MieruLogicalStream> {
        if id == 0 || self.sessions.contains_key(&id) {
            return Err(io::Error::other("mieru duplicate or reserved session ID"));
        }
        if self.sessions.len() >= MAX_SESSIONS {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if !payload.is_empty()
            && (self.policy.max_pending_frames == 0
                || payload.len() > self.policy.max_pending_bytes
                || self.pending_bytes.saturating_add(payload.len())
                    > self.policy.max_connection_pending_bytes)
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let (sender, receiver) = mpsc::channel(RECEIVE_QUEUE);
        let status = Arc::new(Status::default());
        let mut backlog = Backlog::default();
        if !payload.is_empty() {
            let bytes = payload.len();
            sender.try_send(payload).map_err(io::Error::other)?;
            backlog.record_delivery(bytes);
            self.pending_bytes += bytes;
        }
        let stream = MieruLogicalStream::new(
            id,
            status.clone(),
            receiver,
            self.outgoing.clone(),
            self.dropped.clone(),
        );
        self.sessions.insert(
            id,
            Entry {
                status,
                sender,
                backlog,
            },
        );
        Ok(stream)
    }

    pub(crate) fn remove_session(&mut self, id: u32) -> Option<Entry> {
        let entry = self.sessions.remove(&id)?;
        self.pending_bytes = self
            .pending_bytes
            .saturating_sub(entry.backlog.pending_bytes());
        Some(entry)
    }

    pub(crate) async fn run<R: AsyncRead + Unpin>(
        mut self,
        mut socket: R,
        mut cipher: MieruCipher,
        mut dropped: mpsc::UnboundedReceiver<(u32, Arc<Status>)>,
    ) -> io::Result<()> {
        let mut buffer = Vec::new();
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tokio::select! {
                segment = read_segment(&mut socket, &mut buffer, &mut cipher) => {
                    let Some(segment) = segment? else { return Ok(()); };
                    self.dispatch(segment).await?;
                }
                Some((id, status)) = dropped.recv() => {
                    if !self.sessions.get(&id).is_some_and(|entry| Arc::ptr_eq(&entry.status, &status)) { continue; }
                    if let Some(entry) = self.remove_session(id) {
                        if !entry.status.closed() {
                            entry.status.close(None);
                            self.send(Command::Close { id, response: false }).await?;
                        }
                    }
                }
                _ = tick.tick() => self.drain_pending().await?,
            }
        }
    }
    async fn send(&self, command: Command) -> io::Result<()> {
        self.outgoing
            .send(command)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "mieru writer stopped"))
    }
    pub(crate) async fn dispatch(&mut self, segment: Segment) -> io::Result<()> {
        if let Some(meta) = segment.session_meta {
            match meta.protocol_type {
                OPEN_SESSION_REQUEST => {
                    if meta.payload_length > 1024 {
                        return Err(io::Error::other("mieru open payload exceeds limit"));
                    }
                    // Queue the response before making the stream visible: business data
                    // must never precede the open response in the shared nonce sequence.
                    if meta.session_id == 0 || self.sessions.contains_key(&meta.session_id) {
                        return Err(io::Error::other("mieru duplicate or reserved session ID"));
                    }
                    self.send(Command::Open(meta.session_id)).await?;
                    if let Err(error) = self.insert(meta.session_id, segment.payload) {
                        if error.kind() != io::ErrorKind::WouldBlock {
                            return Err(error);
                        }
                        self.send(Command::Close {
                            id: meta.session_id,
                            response: false,
                        })
                        .await?;
                    }
                }
                CLOSE_SESSION_REQUEST | CLOSE_SESSION_RESPONSE => {
                    if let Some(entry) = self.remove_session(meta.session_id) {
                        entry.status.close(None);
                        if meta.protocol_type == CLOSE_SESSION_REQUEST {
                            self.send(Command::Close {
                                id: meta.session_id,
                                response: true,
                            })
                            .await?;
                        } else {
                            self.send(Command::Retire(meta.session_id)).await?;
                        }
                    }
                }
                _ => return Err(io::Error::other("mieru invalid client control direction")),
            }
        } else if let Some(meta) = segment.data_meta {
            if !matches!(
                meta.protocol_type,
                DATA_CLIENT_TO_SERVER | ACK_CLIENT_TO_SERVER
            ) {
                return Err(io::Error::other("mieru invalid client data direction"));
            }
            if meta.protocol_type == ACK_CLIENT_TO_SERVER {
                return Ok(());
            }
            if self.sessions.contains_key(&meta.session_id) {
                if !segment.payload.is_empty() {
                    if self
                        .sessions
                        .get(&meta.session_id)
                        .is_some_and(|entry| !entry.backlog.is_empty())
                    {
                        // Give the bounded delivery queue's consumer one fair turn
                        // before measuring a burst against its backlog budget.
                        tokio::task::yield_now().await;
                    }
                    if !self.buffer_payload(meta.session_id, segment.payload) {
                        self.abort_session(meta.session_id).await?;
                    }
                }
            } else {
                self.send(Command::Close {
                    id: meta.session_id,
                    response: false,
                })
                .await?;
            }
        }
        Ok(())
    }

    fn buffer_payload(&mut self, id: u32, payload: Vec<u8>) -> bool {
        let entry = self.sessions.get_mut(&id).expect("session was checked");
        let drained = entry.backlog.drain_to(&entry.sender, Instant::now());
        self.pending_bytes = self.pending_bytes.saturating_sub(drained.released_bytes);
        if drained.receiver_closed {
            return false;
        }
        let bytes = payload.len();
        if entry.backlog.pending_frames() >= self.policy.max_pending_frames
            || entry.backlog.pending_bytes().saturating_add(bytes) > self.policy.max_pending_bytes
            || self.pending_bytes.saturating_add(bytes) > self.policy.max_connection_pending_bytes
        {
            return false;
        }
        let payload = if entry.backlog.is_empty() {
            match entry.sender.try_send(payload) {
                Ok(()) => {
                    entry.backlog.record_delivery(bytes);
                    self.pending_bytes += bytes;
                    return true;
                }
                Err(mpsc::error::TrySendError::Full(payload)) => payload,
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
            }
        } else {
            payload
        };
        entry.backlog.push_back(payload, Instant::now());
        self.pending_bytes += bytes;
        true
    }

    pub(crate) async fn drain_pending(&mut self) -> io::Result<()> {
        self.drain_pending_at(Instant::now()).await
    }

    async fn drain_pending_at(&mut self, now: Instant) -> io::Result<()> {
        let timeout = Duration::from_millis(self.policy.stall_timeout_ms);
        let mut stalled = Vec::new();
        for (&id, entry) in &mut self.sessions {
            let drained = entry.backlog.drain_to(&entry.sender, now);
            self.pending_bytes = self.pending_bytes.saturating_sub(drained.released_bytes);
            if drained.receiver_closed || entry.backlog.stalled(now, timeout) {
                stalled.push(id);
            }
        }
        for id in stalled {
            self.abort_session(id).await?;
        }
        Ok(())
    }

    async fn abort_session(&mut self, id: u32) -> io::Result<()> {
        let Some(entry) = self.remove_session(id) else {
            return Ok(());
        };
        entry.status.close(Some(io::ErrorKind::ConnectionAborted));
        self.send(Command::Close {
            id,
            response: false,
        })
        .await
    }
}
pub(crate) async fn read_segment<R: AsyncRead + Unpin>(
    socket: &mut R,
    buffer: &mut Vec<u8>,
    cipher: &mut MieruCipher,
) -> io::Result<Option<Segment>> {
    loop {
        match parse_segment(buffer, cipher, false, false) {
            Ok((segment, consumed)) => {
                buffer.drain(..consumed);
                return Ok(Some(segment));
            }
            Err(zero_core::Error::Protocol("mieru: need more data")) => {}
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    error.to_string(),
                ))
            }
        }
        let mut scratch = [0; 8192];
        let n = socket.read(&mut scratch).await?;
        if n == 0 {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                Err(io::ErrorKind::UnexpectedEof.into())
            };
        }
        buffer.extend_from_slice(&scratch[..n]);
    }
}
