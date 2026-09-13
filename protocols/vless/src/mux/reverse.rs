use super::VlessInboundMuxAction;
use crate::reverse::{worker::WorkerStatus, CONTROL_DOMAIN};
use std::{collections::HashSet, sync::Arc};
use zero_core::{Address, Error};

pub(super) struct ReverseMuxState {
    status: Arc<WorkerStatus>,
    controls: HashSet<u16>,
}
impl ReverseMuxState {
    pub(super) fn new(status: Arc<WorkerStatus>) -> Self {
        Self {
            status,
            controls: HashSet::new(),
        }
    }
    pub(super) fn connections(&self, count: usize) {
        self.status.connections(count + self.controls.len());
    }
    /// Internal control streams never enter the application router. Match the
    /// domain exactly, as the reference does, independent of port/network.
    pub(super) fn consume(&mut self, action: &VlessInboundMuxAction) -> Result<bool, Error> {
        match action {
            VlessInboundMuxAction::OpenStream {
                session_id,
                session,
                initial_payload,
                ..
            } if matches!(&session.target, Address::Domain(domain) if domain == CONTROL_DOMAIN) => {
                self.controls.insert(*session_id);
                if !initial_payload.is_empty() {
                    self.status.control(initial_payload)?;
                }
                Ok(true)
            }
            VlessInboundMuxAction::Data {
                session_id,
                payload,
                ..
            } if self.controls.contains(session_id) => {
                if !payload.is_empty() {
                    self.status.control(payload)?;
                }
                Ok(true)
            }
            VlessInboundMuxAction::End { session_id } => Ok(self.controls.remove(session_id)),
            _ => Ok(false),
        }
    }
}
impl Drop for ReverseMuxState {
    fn drop(&mut self) {
        self.status.close();
    }
}
