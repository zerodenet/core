//! Ordered receive window, cumulative ACKs, retransmission and congestion window.
use super::congestion::Cubic;
use crate::{metadata::*, segment::Segment, session::MieruSession};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    num::NonZeroUsize,
    time::Duration,
};
use tokio::time::Instant;
const WINDOW: usize = 128;
const DEFAULT_FRAGMENT_SIZE: usize = 1100;
const MIN_CONGESTION_WINDOW: usize = 16;
const MAX_CONGESTION_WINDOW: usize = 4096;
struct Pending {
    segment: Segment,
    sent: Instant,
    attempts: u32,
    timeout: Duration,
    fast_retransmit: bool,
}
pub struct Transmission {
    pub segment: Segment,
}
pub struct ReliableSession {
    pub(crate) id: u32,
    client: bool,
    pub(crate) next_send: u32,
    pub(crate) next_recv: u32,
    queued: VecDeque<Segment>,
    sent: BTreeMap<u32, Pending>,
    received: BTreeMap<u32, Segment>,
    fragment_size: NonZeroUsize,
    remote_window: usize,
    congestion: Cubic,
    rtt: Option<f64>,
    variance: f64,
    rto: Duration,
    ack: bool,
    last_tx: Instant,
    pub(crate) last_rx: Instant,
    duplicate_ack: u32,
}
impl ReliableSession {
    pub fn new(id: u32, client: bool) -> Self {
        Self::with_fragment_size(
            id,
            client,
            NonZeroUsize::new(DEFAULT_FRAGMENT_SIZE).unwrap(),
        )
    }
    pub(crate) fn with_fragment_size(id: u32, client: bool, fragment_size: NonZeroUsize) -> Self {
        Self {
            id,
            client,
            next_send: 0,
            next_recv: 0,
            queued: VecDeque::new(),
            sent: BTreeMap::new(),
            received: BTreeMap::new(),
            fragment_size,
            remote_window: 32,
            congestion: Cubic::new(MIN_CONGESTION_WINDOW, MAX_CONGESTION_WINDOW),
            rtt: None,
            variance: 0.0,
            rto: Duration::from_millis(300),
            ack: false,
            last_tx: Instant::now(),
            last_rx: Instant::now(),
            duplicate_ack: 0,
        }
    }
    pub fn queued_len(&self) -> usize {
        self.queued.len() + self.sent.len()
    }
    pub fn is_flushed(&self, through: u32) -> bool {
        !self.queued.iter().any(|s| sequence(s) < through)
            && !self.sent.keys().any(|seq| *seq < through)
    }
    pub fn queue_open(&mut self) -> io::Result<()> {
        self.queue_control(if self.client {
            OPEN_SESSION_REQUEST
        } else {
            OPEN_SESSION_RESPONSE
        })
    }
    pub fn queue_control(&mut self, kind: u8) -> io::Result<()> {
        let mut meta = SessionMetadata::new(kind);
        meta.session_id = self.id;
        meta.sequence_number = self.allocate()?;
        self.queued.push_back(Segment {
            session_meta: Some(meta),
            data_meta: None,
            payload: Vec::new(),
        });
        Ok(())
    }
    fn allocate(&mut self) -> io::Result<u32> {
        let seq = self.next_send;
        self.next_send = seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("mieru sequence exhausted"))?;
        Ok(seq)
    }
    pub fn queue_data(&mut self, payload: &[u8]) -> io::Result<()> {
        let fragment_size = self.fragment_size.get();
        let count = payload.len().div_ceil(fragment_size);
        if count > 255 || self.queued_len() + count > 1024 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        for (index, chunk) in payload.chunks(fragment_size).enumerate() {
            let mut m = DataMetadata::new(if self.client {
                DATA_CLIENT_TO_SERVER
            } else {
                DATA_SERVER_TO_CLIENT
            });
            m.session_id = self.id;
            m.sequence_number = self.allocate()?;
            m.payload_length = chunk.len() as u16;
            m.fragment_number = (count - index - 1) as u8;
            self.queued.push_back(Segment {
                session_meta: None,
                data_meta: Some(m),
                payload: chunk.to_vec(),
            });
        }
        Ok(())
    }
    pub fn receive(&mut self, segment: Segment) -> io::Result<()> {
        self.last_rx = Instant::now();
        if let Some(m) = &segment.data_meta {
            self.acknowledge(m.unack_sequence)?;
            self.remote_window = (m.window_size as usize).min(1024);
            if matches!(m.protocol_type, ACK_CLIENT_TO_SERVER | ACK_SERVER_TO_CLIENT) {
                return Ok(());
            }
        }
        let seq = sequence(&segment);
        self.ack = true;
        // The reference advertises additional capacity, not a sequence-number
        // horizon. Retain out-of-order frames beyond WINDOW instead of dropping
        // an entire flight behind one missing packet. Keep the nearest frames
        // when full so the missing head can always make progress.
        if seq >= self.next_recv && !self.received.contains_key(&seq) {
            if self.received.len() == WINDOW {
                let last = *self.received.last_key_value().unwrap().0;
                if seq >= last {
                    return Ok(());
                }
                self.received.pop_last();
            }
            self.received.insert(seq, segment);
        }
        Ok(())
    }
    fn acknowledge(&mut self, ack: u32) -> io::Result<()> {
        // An authenticated peer still cannot acknowledge data never transmitted.
        if ack > self.next_send || self.queued.front().is_some_and(|s| sequence(s) < ack) {
            return Err(io::Error::other("mieru invalid cumulative ACK"));
        }
        let keys: Vec<_> = self.sent.range(..ack).map(|(&seq, _)| seq).collect();
        if keys.is_empty() {
            self.duplicate_ack += 1;
        } else {
            self.duplicate_ack = 0;
        }
        let now = Instant::now();
        for key in keys {
            let pending = self.sent.remove(&key).unwrap();
            if pending.attempts == 1 {
                let sample = pending.sent.elapsed().as_secs_f64();
                if let Some(rtt) = self.rtt {
                    self.variance = 0.75 * self.variance + 0.25 * (rtt - sample).abs();
                    self.rtt = Some(0.875 * rtt + 0.125 * sample);
                } else {
                    self.rtt = Some(sample);
                    self.variance = sample / 2.0;
                }
                self.rto = Duration::from_secs_f64(
                    (self.rtt.unwrap() + 4.0 * self.variance).clamp(0.1, 3.0),
                );
            }
            self.congestion.on_ack(now);
        }
        if self.duplicate_ack >= 3 {
            if let Some(p) = self.sent.get_mut(&ack) {
                p.sent = Instant::now() - p.timeout;
                p.fast_retransmit = true;
            }
            self.duplicate_ack = 0;
        }
        Ok(())
    }
    pub(crate) fn peek(&self) -> Option<&Segment> {
        self.received.get(&self.next_recv)
    }
    pub(crate) fn consume(&mut self) -> io::Result<()> {
        self.received.remove(&self.next_recv);
        self.next_recv = self
            .next_recv
            .checked_add(1)
            .ok_or_else(|| io::Error::other("mieru receive sequence exhausted"))?;
        self.ack = true;
        Ok(())
    }
    pub fn poll_transmissions(&mut self, receive_capacity: usize) -> io::Result<Vec<Transmission>> {
        let now = Instant::now();
        let mut packets = Vec::new();
        let window = receive_capacity.min(WINDOW - self.received.len()) as u16;
        let mut loss = false;
        let mut timeout = false;
        for pending in self.sent.values_mut() {
            if now.duration_since(pending.sent) >= pending.timeout {
                if pending.attempts >= 10 {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                pending.sent = now;
                pending.attempts += 1;
                pending.timeout = (pending.timeout * 2).min(Duration::from_secs(3));
                if pending.fast_retransmit {
                    loss = true;
                    pending.fast_retransmit = false;
                } else {
                    timeout = true;
                }
                packets.push(Transmission {
                    segment: pending.segment.clone(),
                });
            }
        }
        if timeout {
            self.congestion.on_timeout();
        } else if loss {
            self.congestion.on_loss(now);
        }
        // The peer advertises how many additional packets it can receive. It
        // is not a cap on the total number already in flight plus new sends.
        let congestion_capacity = self
            .congestion
            .window_size()
            .saturating_sub(self.sent.len());
        let mut send_capacity = self.remote_window.min(congestion_capacity);
        if send_capacity == 0
            && self.remote_window == 0
            && congestion_capacity > 0
            && self.queued.front().is_some_and(|s| s.data_meta.is_none())
        {
            // Session control must still be able to close a zero-window peer.
            send_capacity = 1;
        }
        while send_capacity > 0 {
            let Some(segment) = self.queued.pop_front() else {
                break;
            };
            self.sent.insert(
                sequence(&segment),
                Pending {
                    segment: segment.clone(),
                    sent: now,
                    attempts: 1,
                    timeout: self.rto,
                    fast_retransmit: false,
                },
            );
            packets.push(Transmission { segment });
            send_capacity -= 1;
        }
        if self.ack || now.duration_since(self.last_tx) > Duration::from_secs(2) {
            let mut meta = DataMetadata::new(if self.client {
                ACK_CLIENT_TO_SERVER
            } else {
                ACK_SERVER_TO_CLIENT
            });
            meta.session_id = self.id;
            meta.sequence_number = self.next_send.saturating_sub(1);
            packets.push(Transmission {
                segment: Segment {
                    session_meta: None,
                    data_meta: Some(meta),
                    payload: Vec::new(),
                },
            });
            self.ack = false;
        }
        for tx in &mut packets {
            if let Some(m) = &mut tx.segment.session_meta {
                m.timestamp = MieruSession::timestamp_minutes();
            }
            if let Some(m) = &mut tx.segment.data_meta {
                m.timestamp = MieruSession::timestamp_minutes();
                m.unack_sequence = self.next_recv;
                m.window_size = window;
            }
        }
        if !packets.is_empty() {
            self.last_tx = now;
        }
        Ok(packets)
    }

    #[cfg(test)]
    pub(crate) fn congestion_window(&self) -> usize {
        self.congestion.window_size()
    }

    #[cfg(test)]
    pub(crate) fn expire_retransmission_timers(&mut self) {
        let now = Instant::now();
        for pending in self.sent.values_mut() {
            pending.sent = now - pending.timeout;
            pending.fast_retransmit = false;
        }
    }
}
pub(crate) fn sequence(s: &Segment) -> u32 {
    s.session_meta.as_ref().map_or_else(
        || s.data_meta.as_ref().unwrap().sequence_number,
        |m| m.sequence_number,
    )
}
