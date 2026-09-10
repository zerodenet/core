use std::{collections::VecDeque, time::Duration};
use tokio::{sync::mpsc, time::Instant};

pub(super) struct DrainResult {
    pub(super) released_bytes: usize,
    pub(super) receiver_closed: bool,
}

#[derive(Default)]
pub(super) struct Backlog {
    frames: VecDeque<Vec<u8>>,
    bytes: usize,
    delivered: VecDeque<usize>,
    delivered_bytes: usize,
    stalled_since: Option<Instant>,
}

impl Backlog {
    pub(super) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    #[cfg(test)]
    pub(super) fn frames(&self) -> usize {
        self.frames.len()
    }

    pub(super) fn pending_frames(&self) -> usize {
        self.frames.len() + self.delivered.len()
    }

    pub(super) fn pending_bytes(&self) -> usize {
        self.bytes + self.delivered_bytes
    }

    pub(super) fn record_delivery(&mut self, bytes: usize) {
        self.delivered.push_back(bytes);
        self.delivered_bytes += bytes;
    }

    pub(super) fn release_consumed(&mut self, outstanding: usize) -> usize {
        let mut released = 0;
        while self.delivered.len() > outstanding {
            let bytes = self.delivered.pop_front().unwrap();
            self.delivered_bytes -= bytes;
            released += bytes;
        }
        released
    }

    pub(super) fn drain_to(&mut self, sender: &mpsc::Sender<Vec<u8>>, now: Instant) -> DrainResult {
        let outstanding = sender.max_capacity() - sender.capacity();
        let released_bytes = self.release_consumed(outstanding);
        let mut progressed = released_bytes > 0;
        let mut receiver_closed = false;
        while let Some(payload) = self.pop_front() {
            let bytes = payload.len();
            match sender.try_send(payload) {
                Ok(()) => {
                    self.record_delivery(bytes);
                    progressed = true;
                }
                Err(mpsc::error::TrySendError::Full(payload)) => {
                    self.push_front(payload);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(payload)) => {
                    self.push_front(payload);
                    receiver_closed = true;
                    break;
                }
            }
        }
        if progressed {
            self.note_progress(now);
        }
        DrainResult {
            released_bytes,
            receiver_closed,
        }
    }

    pub(super) fn push_back(&mut self, payload: Vec<u8>, now: Instant) {
        self.bytes += payload.len();
        self.frames.push_back(payload);
        self.stalled_since.get_or_insert(now);
    }

    fn push_front(&mut self, payload: Vec<u8>) {
        self.bytes += payload.len();
        self.frames.push_front(payload);
    }

    fn pop_front(&mut self) -> Option<Vec<u8>> {
        let payload = self.frames.pop_front()?;
        self.bytes -= payload.len();
        Some(payload)
    }

    fn note_progress(&mut self, now: Instant) {
        if self.frames.is_empty() {
            self.stalled_since = None;
        } else {
            self.stalled_since = Some(now);
        }
    }

    pub(super) fn stalled(&self, now: Instant, timeout: Duration) -> bool {
        self.stalled_since
            .is_some_and(|since| now.duration_since(since) >= timeout)
    }

    #[cfg(test)]
    pub(super) fn stalled_since(&self) -> Option<Instant> {
        self.stalled_since
    }
}
