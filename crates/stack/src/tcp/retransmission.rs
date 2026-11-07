use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::Mutex;
use std::task::Waker;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Notify};

use super::{output::CachedPacket, sequence_after, sequence_before};
use zero_traits::PacketDeriveError;

// This TCP leg only crosses the local TUN device, so its retransmission
// timers can be substantially tighter than an Internet-facing TCP socket.
const INITIAL_RETRANSMISSION_TIMEOUT: Duration = Duration::from_millis(250);
const MIN_RETRANSMISSION_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(2);
const NETWORK_INITIAL_RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(1);
const NETWORK_MAX_RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(10);
const OUTBOUND_QUEUE_RETRY_DELAY: Duration = Duration::from_millis(10);
const ZERO_WINDOW_PROBE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RETRANSMISSIONS: u8 = 5;

const SEND_FAILURE_NONE: u8 = 0;
const SEND_FAILURE_TIMED_OUT: u8 = 1;
const SEND_FAILURE_TRANSPORT_CLOSED: u8 = 2;
const SEND_FAILURE_UNREACHABLE: u8 = 3;

pub(super) struct TcpSendControl {
    snd_una: AtomicU32,
    peer_window: AtomicU32,
    congestion_window: AtomicU32,
    slow_start_threshold: AtomicU32,
    segment_size: u32,
    path_mss: AtomicU32,
    max_retransmission_timeout: Duration,
    peer_reset: AtomicBool,
    send_failure: AtomicU8,
    storage_failure: Mutex<Option<PacketDeriveError>>,
    writer_waker: Mutex<Option<Waker>>,
    reader_waker: Mutex<Option<Waker>>,
    retransmission: Mutex<RetransmissionState>,
    pub(super) retransmission_notify: Notify,
}

struct RetransmissionState {
    segments: VecDeque<RetransmissionSegment>,
    estimator: RtoEstimator,
    duplicate_acks: u8,
    persist_packet: Option<CachedPacket>,
    persist_retry_at: Instant,
    stopped: bool,
}

struct RetransmissionSegment {
    sequence_end: u32,
    packet: CachedPacket,
    first_sent_at: Instant,
    retry_at: Instant,
    timeout: Duration,
    retransmissions: u8,
    was_retransmitted: bool,
}

struct RtoEstimator {
    smoothed_rtt_micros: Option<u64>,
    rtt_variance_micros: u64,
    timeout: Duration,
    maximum: Duration,
}

impl RtoEstimator {
    fn new(initial: Duration, maximum: Duration) -> Self {
        Self {
            smoothed_rtt_micros: None,
            rtt_variance_micros: 0,
            timeout: initial,
            maximum,
        }
    }

    fn observe(&mut self, sample: Duration) {
        let sample = duration_micros(sample);
        let smoothed = match self.smoothed_rtt_micros {
            None => {
                self.rtt_variance_micros = sample / 2;
                sample
            }
            Some(smoothed) => {
                let deviation = smoothed.abs_diff(sample);
                self.rtt_variance_micros = self
                    .rtt_variance_micros
                    .saturating_mul(3)
                    .saturating_add(deviation)
                    / 4;
                smoothed.saturating_mul(7).saturating_add(sample) / 8
            }
        };
        self.smoothed_rtt_micros = Some(smoothed);
        let timeout_micros =
            smoothed.saturating_add(self.rtt_variance_micros.saturating_mul(4).max(1_000));
        self.timeout =
            clamp_retransmission_timeout(Duration::from_micros(timeout_micros), self.maximum);
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn clamp_retransmission_timeout(timeout: Duration, maximum: Duration) -> Duration {
    timeout.clamp(MIN_RETRANSMISSION_TIMEOUT, maximum)
}

pub(super) enum RetransmissionWait {
    Stopped,
    Idle,
    Delay(Duration),
}

pub(super) enum RetransmissionResult {
    Continue,
    Failed,
}

impl TcpSendControl {
    pub(super) fn new(snd_una: u32, peer_window: u16, segment_size: u16) -> Self {
        Self::with_timing(
            snd_una,
            peer_window,
            segment_size,
            INITIAL_RETRANSMISSION_TIMEOUT,
            MAX_RETRANSMISSION_TIMEOUT,
        )
    }

    pub(super) fn new_network(snd_una: u32, peer_window: u16, segment_size: u16) -> Self {
        Self::with_timing(
            snd_una,
            peer_window,
            segment_size,
            NETWORK_INITIAL_RETRANSMISSION_TIMEOUT,
            NETWORK_MAX_RETRANSMISSION_TIMEOUT,
        )
    }

    fn with_timing(
        snd_una: u32,
        peer_window: u16,
        segment_size: u16,
        initial_timeout: Duration,
        max_retransmission_timeout: Duration,
    ) -> Self {
        let segment_size = u32::from(segment_size.max(1));
        let initial_congestion_window = segment_size
            .saturating_mul(10)
            .min(segment_size.saturating_mul(2).max(14_600));
        Self {
            snd_una: AtomicU32::new(snd_una),
            peer_window: AtomicU32::new(u32::from(peer_window)),
            congestion_window: AtomicU32::new(initial_congestion_window),
            slow_start_threshold: AtomicU32::new(u32::MAX),
            segment_size,
            path_mss: AtomicU32::new(segment_size),
            max_retransmission_timeout,
            peer_reset: AtomicBool::new(false),
            send_failure: AtomicU8::new(SEND_FAILURE_NONE),
            storage_failure: Mutex::new(None),
            writer_waker: Mutex::new(None),
            reader_waker: Mutex::new(None),
            retransmission: Mutex::new(RetransmissionState {
                segments: VecDeque::new(),
                estimator: RtoEstimator::new(initial_timeout, max_retransmission_timeout),
                duplicate_acks: 0,
                persist_packet: None,
                persist_retry_at: Instant::now() + ZERO_WINDOW_PROBE_INTERVAL,
                stopped: false,
            }),
            retransmission_notify: Notify::new(),
        }
    }

    pub(super) fn observe_ack(&self, acknowledgement: u32, window: u16, snd_nxt: u32) {
        let snd_una = self.snd_una.load(Ordering::Acquire);
        if !sequence_before(acknowledgement, snd_una) && !sequence_after(acknowledgement, snd_nxt) {
            self.snd_una.store(acknowledgement, Ordering::Release);
            self.peer_window.store(u32::from(window), Ordering::Release);
        } else {
            return;
        }

        if sequence_after(acknowledgement, snd_una) {
            self.grow_congestion_window(acknowledgement.wrapping_sub(snd_una));
            let now = Instant::now();
            let mut retired = Vec::new();
            let mut retransmission = self
                .retransmission
                .lock()
                .expect("TCP retransmission lock poisoned");
            let mut rtt_sample = None;
            while retransmission
                .segments
                .front()
                .is_some_and(|segment| !sequence_before(acknowledgement, segment.sequence_end))
            {
                let segment = retransmission
                    .segments
                    .pop_front()
                    .expect("front retransmission segment present");
                if rtt_sample.is_none() && !segment.was_retransmitted {
                    rtt_sample = Some(now.saturating_duration_since(segment.first_sent_at));
                }
                retired.push(segment);
            }
            if let Some(sample) = rtt_sample {
                retransmission.estimator.observe(sample);
            }
            retransmission.duplicate_acks = 0;
        } else if acknowledgement == snd_una {
            let mut retransmission = self
                .retransmission
                .lock()
                .expect("TCP retransmission lock poisoned");
            if !retransmission.segments.is_empty() {
                retransmission.duplicate_acks = retransmission.duplicate_acks.saturating_add(1);
                if retransmission.duplicate_acks == 3 {
                    let outstanding = snd_nxt.wrapping_sub(snd_una);
                    let threshold = (outstanding / 2).max(self.segment_size.saturating_mul(2));
                    self.slow_start_threshold
                        .store(threshold, Ordering::Release);
                    self.congestion_window.store(
                        threshold.saturating_add(self.segment_size.saturating_mul(3)),
                        Ordering::Release,
                    );
                    if let Some(segment) = retransmission.segments.front_mut() {
                        segment.retry_at = Instant::now();
                    }
                }
            }
        }
        self.retransmission_notify.notify_one();
        self.wake_writer();
    }

    pub(super) fn available_window(&self, snd_nxt: u32) -> u32 {
        let outstanding = snd_nxt.wrapping_sub(self.snd_una.load(Ordering::Acquire));
        self.peer_window
            .load(Ordering::Acquire)
            .min(self.congestion_window.load(Ordering::Acquire))
            .saturating_sub(outstanding)
    }

    pub(super) fn reduce_path_mss(&self, mss: u16) {
        self.path_mss
            .fetch_min(u32::from(mss.max(1)), Ordering::AcqRel);
    }

    pub(super) fn path_mss(&self) -> u16 {
        self.path_mss.load(Ordering::Acquire) as u16
    }

    fn grow_congestion_window(&self, acknowledged: u32) {
        let congestion_window = self.congestion_window.load(Ordering::Acquire);
        let threshold = self.slow_start_threshold.load(Ordering::Acquire);
        let increase = if congestion_window < threshold {
            acknowledged.min(self.segment_size)
        } else {
            self.segment_size
                .saturating_mul(self.segment_size)
                .checked_div(congestion_window.max(1))
                .unwrap_or(1)
                .max(1)
        };
        self.congestion_window.store(
            congestion_window.saturating_add(increase),
            Ordering::Release,
        );
    }

    pub(super) fn register_writer(&self, waker: &Waker) {
        *self
            .writer_waker
            .lock()
            .expect("TCP send-control lock poisoned") = Some(waker.clone());
    }

    pub(super) fn register_reader(&self, waker: &Waker) {
        *self
            .reader_waker
            .lock()
            .expect("TCP send-control lock poisoned") = Some(waker.clone());
    }

    fn wake_writer(&self) {
        let waker = self
            .writer_waker
            .lock()
            .expect("TCP send-control lock poisoned")
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) fn wake_io(&self) {
        self.wake_writer();
        let waker = self
            .reader_waker
            .lock()
            .expect("TCP send-control lock poisoned")
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) fn io_error(&self) -> Option<io::Error> {
        if let Some(source) = *self.storage_failure.lock().expect("TCP storage fault lock") {
            return Some(io::Error::other(source));
        }
        if self.peer_reset.load(Ordering::Acquire) {
            return Some(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "TCP connection reset by peer",
            ));
        }
        match self.send_failure.load(Ordering::Acquire) {
            SEND_FAILURE_TIMED_OUT => Some(io::Error::new(
                io::ErrorKind::TimedOut,
                "TCP acknowledgement timed out",
            )),
            SEND_FAILURE_TRANSPORT_CLOSED => Some(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TCP packet transport closed",
            )),
            SEND_FAILURE_UNREACHABLE => Some(io::Error::new(
                io::ErrorKind::NetworkUnreachable,
                "TCP destination unreachable",
            )),
            _ => None,
        }
    }

    pub(super) fn track_segment(&self, sequence_end: u32, packet: impl Into<CachedPacket>) {
        let now = Instant::now();
        let mut retransmission = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        if retransmission.stopped {
            return;
        }
        let timeout = retransmission.estimator.timeout;
        retransmission.segments.push_back(RetransmissionSegment {
            sequence_end,
            packet: packet.into(),
            first_sent_at: now,
            retry_at: now + timeout,
            timeout,
            retransmissions: 0,
            was_retransmitted: false,
        });
        drop(retransmission);
        self.retransmission_notify.notify_one();
    }

    pub(super) fn set_persist_spec(
        &self,
        spec: crate::packet::TcpPacketSpec<'_>,
        output: &crate::packet_output::PacketSender,
    ) -> Result<(), PacketDeriveError> {
        {
            let mut state = self
                .retransmission
                .lock()
                .expect("TCP retransmission lock poisoned");
            if state.stopped {
                return Ok(());
            }
            if let Some(packet) = &mut state.persist_packet {
                if packet.update(spec)? {
                    state.persist_retry_at = Instant::now() + ZERO_WINDOW_PROBE_INTERVAL;
                    drop(state);
                    self.retransmission_notify.notify_one();
                    return Ok(());
                }
            }
        }
        // The prior view stays alive if it still has actual output custody.
        // Build the replacement outside the owner's lock; failure keeps it.
        let packet = CachedPacket::build(spec, output)?;
        let mut state = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        if state.stopped {
            drop(state);
            drop(packet);
            return Ok(());
        }
        let previous = state.persist_packet.replace(packet);
        state.persist_retry_at = Instant::now() + ZERO_WINDOW_PROBE_INTERVAL;
        drop(state);
        drop(previous);
        self.retransmission_notify.notify_one();
        Ok(())
    }

    pub(super) fn retry_now(&self) {
        let mut retransmission = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        if let Some(segment) = retransmission.segments.front_mut() {
            segment.retry_at = Instant::now();
        }
        drop(retransmission);
        self.retransmission_notify.notify_one();
    }

    pub(super) fn retransmission_wait(&self, now: Instant) -> RetransmissionWait {
        let retransmission = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        if retransmission.stopped {
            return RetransmissionWait::Stopped;
        }
        match retransmission.segments.front() {
            Some(segment) => {
                RetransmissionWait::Delay(segment.retry_at.saturating_duration_since(now))
            }
            None if self.peer_window.load(Ordering::Acquire) == 0
                && retransmission.persist_packet.is_some() =>
            {
                RetransmissionWait::Delay(
                    retransmission
                        .persist_retry_at
                        .saturating_duration_since(now),
                )
            }
            None => RetransmissionWait::Idle,
        }
    }

    pub(super) fn retransmit_due(
        &self,
        outbound: &crate::packet_output::PacketSender,
        now: Instant,
    ) -> RetransmissionResult {
        // Actual payload drops/wakes follow the retransmission guard on every
        // return path; no external memory waiter runs under this owner lock.
        let mut returned_outputs = Vec::new();
        let mut retired_segments = Vec::new();
        let mut retransmission = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        let Some(segment) = retransmission.segments.front_mut() else {
            if self.peer_window.load(Ordering::Acquire) != 0
                || retransmission.persist_retry_at > now
            {
                return RetransmissionResult::Continue;
            }
            let Some(packet) = retransmission.persist_packet.clone() else {
                return RetransmissionResult::Continue;
            };
            let packet = match packet.outgoing() {
                Ok(packet) => packet,
                Err(PacketDeriveError::Busy) => {
                    retransmission.persist_retry_at = now + OUTBOUND_QUEUE_RETRY_DELAY;
                    return RetransmissionResult::Continue;
                }
                Err(source) => {
                    *self.storage_failure.lock().expect("TCP storage fault lock") = Some(source);
                    retransmission.stopped = true;
                    return RetransmissionResult::Failed;
                }
            };
            return match outbound.try_send_buffer(packet) {
                Ok(()) => {
                    retransmission.persist_retry_at = now + ZERO_WINDOW_PROBE_INTERVAL;
                    RetransmissionResult::Continue
                }
                Err(mpsc::error::TrySendError::Full(packet)) => {
                    returned_outputs.push(packet);
                    retransmission.persist_retry_at = now + OUTBOUND_QUEUE_RETRY_DELAY;
                    RetransmissionResult::Continue
                }
                Err(mpsc::error::TrySendError::Closed(packet)) => {
                    returned_outputs.push(packet);
                    retransmission.stopped = true;
                    self.send_failure
                        .store(SEND_FAILURE_TRANSPORT_CLOSED, Ordering::Release);
                    RetransmissionResult::Failed
                }
            };
        };
        if segment.retry_at > now {
            return RetransmissionResult::Continue;
        }
        if segment.retransmissions >= MAX_RETRANSMISSIONS {
            retransmission.stopped = true;
            retired_segments.extend(retransmission.segments.drain(..));
            self.send_failure
                .store(SEND_FAILURE_TIMED_OUT, Ordering::Release);
            return RetransmissionResult::Failed;
        }
        if segment.retransmissions == 0 {
            let outstanding = segment
                .sequence_end
                .wrapping_sub(self.snd_una.load(Ordering::Acquire));
            self.slow_start_threshold.store(
                (outstanding / 2).max(self.segment_size.saturating_mul(2)),
                Ordering::Release,
            );
            self.congestion_window
                .store(self.segment_size, Ordering::Release);
        }
        let packet = match segment.packet.outgoing() {
            Ok(packet) => packet,
            Err(PacketDeriveError::Busy) => {
                segment.retry_at = now + OUTBOUND_QUEUE_RETRY_DELAY;
                return RetransmissionResult::Continue;
            }
            Err(source) => {
                *self.storage_failure.lock().expect("TCP storage fault lock") = Some(source);
                retransmission.stopped = true;
                return RetransmissionResult::Failed;
            }
        };
        match outbound.try_send_buffer(packet) {
            Ok(()) => {
                segment.retransmissions += 1;
                segment.was_retransmitted = true;
                segment.timeout = clamp_retransmission_timeout(
                    segment.timeout.saturating_mul(2),
                    self.max_retransmission_timeout,
                );
                segment.retry_at = now + segment.timeout;
                RetransmissionResult::Continue
            }
            Err(mpsc::error::TrySendError::Full(packet)) => {
                returned_outputs.push(packet);
                segment.retry_at = now + OUTBOUND_QUEUE_RETRY_DELAY;
                RetransmissionResult::Continue
            }
            Err(mpsc::error::TrySendError::Closed(packet)) => {
                returned_outputs.push(packet);
                retransmission.stopped = true;
                retired_segments.extend(retransmission.segments.drain(..));
                self.send_failure
                    .store(SEND_FAILURE_TRANSPORT_CLOSED, Ordering::Release);
                RetransmissionResult::Failed
            }
        }
    }

    pub(super) fn stop(&self) {
        let mut retransmission = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        retransmission.stopped = true;
        let segments = std::mem::take(&mut retransmission.segments);
        let persist = retransmission.persist_packet.take();
        drop(retransmission);
        drop(segments);
        drop(persist);
        self.retransmission_notify.notify_one();
        self.wake_io();
    }

    pub(super) fn observe_transport_closed(&self) {
        self.send_failure
            .store(SEND_FAILURE_TRANSPORT_CLOSED, Ordering::Release);
        self.stop();
    }

    pub(super) fn expire(&self) {
        self.send_failure
            .store(SEND_FAILURE_TIMED_OUT, Ordering::Release);
        self.stop();
    }

    pub(super) fn observe_reset(&self) {
        self.peer_reset.store(true, Ordering::Release);
        self.stop();
    }

    pub(super) fn observe_unreachable(&self) {
        self.send_failure
            .store(SEND_FAILURE_UNREACHABLE, Ordering::Release);
        self.stop();
    }
}
