use super::*;
use crate::{metadata::*, segment::Segment};
impl Driver {
    pub(crate) async fn receive(&mut self, segment: Segment) -> io::Result<()> {
        let (id, kind) = segment.session_meta.as_ref().map_or_else(
            || {
                let m = segment.data_meta.as_ref().unwrap();
                (m.session_id, m.protocol_type)
            },
            |m| (m.session_id, m.protocol_type),
        );
        let allowed = matches!(kind, CLOSE_SESSION_REQUEST | CLOSE_SESSION_RESPONSE)
            || if self.client {
                matches!(
                    kind,
                    OPEN_SESSION_RESPONSE | DATA_SERVER_TO_CLIENT | ACK_SERVER_TO_CLIENT
                )
            } else {
                matches!(
                    kind,
                    OPEN_SESSION_REQUEST | DATA_CLIENT_TO_SERVER | ACK_CLIENT_TO_SERVER
                )
            };
        if matches!(kind, OPEN_SESSION_REQUEST | OPEN_SESSION_RESPONSE)
            && segment
                .session_meta
                .as_ref()
                .is_some_and(|m| m.sequence_number != 0)
        {
            return Ok(());
        }
        if !allowed {
            return Ok(());
        }
        if let Some(tomb) = self.retired.get(&id) {
            if kind == CLOSE_SESSION_REQUEST {
                self.close_response(id, tomb.sequence).await?;
            }
            return Ok(());
        }
        if !self.states.contains_key(&id) {
            if kind != OPEN_SESSION_REQUEST || self.client || self.retired.len() >= 4096 {
                return Ok(());
            }
            if self.start(id).is_err() {
                return Ok(());
            }
            if self.book.insert(id, Vec::new()).is_err() {
                self.retire(id, Retirement::Failed(io::ErrorKind::ConnectionAborted));
                return Ok(());
            }
            self.states.get_mut(&id).unwrap().reliable.queue_open()?;
        }
        if kind == OPEN_SESSION_RESPONSE
            && segment
                .session_meta
                .as_ref()
                .is_some_and(|m| m.sequence_number == 0 && m.status_code == 0)
        {
            if let Some(entry) = self.book.sessions.get(&id) {
                entry.status.mark_open();
            }
        }
        if kind == CLOSE_SESSION_RESPONSE {
            if self.states[&id].closing.is_some() {
                self.retire(id, Retirement::LocalCloseAcknowledged);
            }
            return Ok(());
        }
        if segment
            .session_meta
            .as_ref()
            .is_some_and(|m| m.status_code != 0)
        {
            self.retire(id, Retirement::Failed(io::ErrorKind::PermissionDenied));
            return Ok(());
        }
        if self
            .states
            .get_mut(&id)
            .unwrap()
            .reliable
            .receive(segment)
            .is_err()
        {
            self.retire(id, Retirement::Failed(io::ErrorKind::InvalidData));
        }
        self.drain(id).await
    }
    pub(super) async fn drain(&mut self, id: u32) -> io::Result<()> {
        loop {
            let Some(state) = self.states.get_mut(&id) else {
                return Ok(());
            };
            let Some(segment) = state.reliable.peek() else {
                return Ok(());
            };
            if segment
                .session_meta
                .as_ref()
                .is_some_and(|m| m.protocol_type == CLOSE_SESSION_REQUEST)
            {
                let sequence = state.reliable.next_send;
                self.close_response(id, sequence).await?;
                self.retire(id, Retirement::PeerClosed);
                return Ok(());
            }
            if !segment.payload.is_empty() {
                let Some(entry) = self.book.sessions.get(&id) else {
                    return Ok(());
                };
                match entry.sender.try_send(segment.payload.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => return Ok(()),
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Ok(());
                    }
                }
            }
            state.reliable.consume()?;
        }
    }
    async fn close_response(&self, id: u32, sequence: u32) -> io::Result<()> {
        let mut m = SessionMetadata::new(CLOSE_SESSION_RESPONSE);
        m.session_id = id;
        m.sequence_number = sequence;
        m.timestamp = crate::session::MieruSession::timestamp_minutes();
        self.io
            .send(&self.codec.encode(&Segment {
                session_meta: Some(m),
                data_meta: None,
                payload: Vec::new(),
            })?)
            .await
    }
}
