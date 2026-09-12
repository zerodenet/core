//! Protocol-owned sender identity, monotonic counters and replay admission.
use crate::{shared::random_u64, shared::ReplayWindow};
use std::collections::HashMap;
#[cfg(test)]
use std::time::Duration;
use tokio::time::Instant;
use zero_core::Error;

#[cfg(test)]
pub(crate) const RETENTION: Duration = Duration::from_secs(300);
#[cfg(test)]
pub(crate) const CAPACITY: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct SenderSession {
    id: Option<u64>,
    packet: u64,
}
impl SenderSession {
    pub(crate) fn next(&mut self) -> Result<(u64, u64), Error> {
        let id = match self.id {
            Some(id) => id,
            None => {
                let id = random_u64()?;
                self.id = Some(id);
                id
            }
        };
        // A caller must recreate the flow on exhaustion; never wrap/reuse a nonce.
        self.packet = self
            .packet
            .checked_add(1)
            .filter(|packet| *packet < u64::MAX)
            .ok_or(Error::Protocol("ss: UDP packet counter exhausted"))?;
        Ok((id, self.packet))
    }
    pub(crate) fn matches(&self, id: u64) -> bool {
        self.id == Some(id)
    }
}

#[derive(Default)]
pub(crate) struct ReceiveWindows(
    HashMap<u64, (ReplayWindow, Instant)>,
    crate::validation::StateLimits,
);
impl ReceiveWindows {
    pub(crate) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self(Default::default(), limits)
    }
    pub(crate) fn accept(&mut self, session: u64, packet: u64) -> Result<bool, Error> {
        if packet == u64::MAX {
            return Ok(false);
        }
        self.prune();
        if !self.0.contains_key(&session)
            && self
                .1
                .udp_capacity
                .is_some_and(|capacity| self.0.len() >= capacity)
        {
            return Err(Error::Protocol("ss: UDP replay session capacity exhausted"));
        }
        let (window, touched) = self
            .0
            .entry(session)
            .or_insert_with(|| (ReplayWindow::new(), Instant::now()));
        let accepted = window.check_and_update(packet);
        if accepted {
            *touched = Instant::now();
        }
        Ok(accepted)
    }
    pub(crate) fn prune(&mut self) {
        self.0
            .retain(|_, (_, seen)| seen.elapsed() < self.1.udp_replay_retention());
    }
}
impl std::fmt::Debug for ReceiveWindows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveWindows")
            .field("sessions", &self.0.len())
            .finish()
    }
}

#[derive(Default)]
pub(crate) struct ResponseSessions(
    HashMap<u64, (SenderSession, Instant)>,
    crate::validation::StateLimits,
);
impl ResponseSessions {
    pub(crate) fn with_limits(limits: crate::validation::StateLimits) -> Self {
        Self(Default::default(), limits)
    }
    pub(crate) fn next(&mut self, client: u64) -> Result<(u64, u64), Error> {
        self.prune();
        if !self.0.contains_key(&client)
            && self
                .1
                .udp_capacity
                .is_some_and(|capacity| self.0.len() >= capacity)
        {
            return Err(Error::Protocol(
                "ss: UDP response session capacity exhausted",
            ));
        }
        let (session, touched) = self
            .0
            .entry(client)
            .or_insert_with(|| (SenderSession::default(), Instant::now()));
        *touched = Instant::now();
        session.next()
    }
    pub(crate) fn prune(&mut self) {
        self.0
            .retain(|_, (_, seen)| seen.elapsed() < self.1.udp_idle());
    }
}

#[cfg(test)]
#[path = "../../tests/udp_session/state.rs"]
mod tests;
