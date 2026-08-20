use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::Mutex;
use std::task::Waker;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Notify};

use super::{sequence_after, sequence_before};

// This TCP leg only crosses the local TUN device, so its retransmission
// timers can be substantially tighter than an Internet-facing TCP socket.
const INITIAL_RETRANSMISSION_TIMEOUT: Duration = Duration::from_millis(250);
const MIN_RETRANSMISSION_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(2);
const OUTBOUND_QUEUE_RETRY_DELAY: Duration = Duration::from_millis(10);
const MAX_RETRANSMISSIONS: u8 = 5;

const SEND_FAILURE_NONE: u8 = 0;
const SEND_FAILURE_TIMED_OUT: u8 = 1;
const SEND_FAILURE_TRANSPORT_CLOSED: u8 = 2;

pub(super) struct TcpSendControl {
    snd_una: AtomicU32,
    peer_window: AtomicU32,
    peer_reset: AtomicBool,
    send_failure: AtomicU8,
    writer_waker: Mutex<Option<Waker>>,
    reader_waker: Mutex<Option<Waker>>,
    retransmission: Mutex<RetransmissionState>,
    pub(super) retransmission_notify: Notify,
}

struct RetransmissionState {
    segments: VecDeque<RetransmissionSegment>,
    estimator: RtoEstimator,
    stopped: bool,
}

struct RetransmissionSegment {
    sequence_end: u32,
    packet: Vec<u8>,
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
}

impl RtoEstimator {
    fn new() -> Self {
        Self {
            smoothed_rtt_micros: None,
            rtt_variance_micros: 0,
            timeout: INITIAL_RETRANSMISSION_TIMEOUT,
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
        self.timeout = clamp_retransmission_timeout(Duration::from_micros(timeout_micros));
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn clamp_retransmission_timeout(timeout: Duration) -> Duration {
    timeout.clamp(MIN_RETRANSMISSION_TIMEOUT, MAX_RETRANSMISSION_TIMEOUT)
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
    pub(super) fn new(snd_una: u32, peer_window: u16) -> Self {
        Self {
            snd_una: AtomicU32::new(snd_una),
            peer_window: AtomicU32::new(u32::from(peer_window)),
            peer_reset: AtomicBool::new(false),
            send_failure: AtomicU8::new(SEND_FAILURE_NONE),
            writer_waker: Mutex::new(None),
            reader_waker: Mutex::new(None),
            retransmission: Mutex::new(RetransmissionState {
                segments: VecDeque::new(),
                estimator: RtoEstimator::new(),
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
            let now = Instant::now();
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
            }
            if let Some(sample) = rtt_sample {
                retransmission.estimator.observe(sample);
            }
        }
        self.retransmission_notify.notify_one();
        self.wake_writer();
    }

    pub(super) fn available_window(&self, snd_nxt: u32) -> u32 {
        let outstanding = snd_nxt.wrapping_sub(self.snd_una.load(Ordering::Acquire));
        self.peer_window
            .load(Ordering::Acquire)
            .saturating_sub(outstanding)
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
        if let Some(waker) = self
            .writer_waker
            .lock()
            .expect("TCP send-control lock poisoned")
            .take()
        {
            waker.wake();
        }
    }

    pub(super) fn wake_io(&self) {
        self.wake_writer();
        if let Some(waker) = self
            .reader_waker
            .lock()
            .expect("TCP send-control lock poisoned")
            .take()
        {
            waker.wake();
        }
    }

    pub(super) fn io_error(&self) -> Option<io::Error> {
        if self.peer_reset.load(Ordering::Acquire) {
            return Some(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection reset by local client",
            ));
        }
        match self.send_failure.load(Ordering::Acquire) {
            SEND_FAILURE_TIMED_OUT => Some(io::Error::new(
                io::ErrorKind::TimedOut,
                "local TUN TCP acknowledgement timed out",
            )),
            SEND_FAILURE_TRANSPORT_CLOSED => Some(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "local TUN packet transport closed",
            )),
            _ => None,
        }
    }

    pub(super) fn track_segment(&self, sequence_end: u32, packet: Vec<u8>) {
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
            packet,
            first_sent_at: now,
            retry_at: now + timeout,
            timeout,
            retransmissions: 0,
            was_retransmitted: false,
        });
        drop(retransmission);
        self.retransmission_notify.notify_one();
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
            None => RetransmissionWait::Idle,
        }
    }

    pub(super) fn retransmit_due(
        &self,
        outbound: &mpsc::Sender<Vec<u8>>,
        now: Instant,
    ) -> RetransmissionResult {
        let mut retransmission = self
            .retransmission
            .lock()
            .expect("TCP retransmission lock poisoned");
        let Some(segment) = retransmission.segments.front_mut() else {
            return RetransmissionResult::Continue;
        };
        if segment.retry_at > now {
            return RetransmissionResult::Continue;
        }
        if segment.retransmissions >= MAX_RETRANSMISSIONS {
            retransmission.stopped = true;
            retransmission.segments.clear();
            self.send_failure
                .store(SEND_FAILURE_TIMED_OUT, Ordering::Release);
            return RetransmissionResult::Failed;
        }
        match outbound.try_send(segment.packet.clone()) {
            Ok(()) => {
                segment.retransmissions += 1;
                segment.was_retransmitted = true;
                segment.timeout = clamp_retransmission_timeout(segment.timeout.saturating_mul(2));
                segment.retry_at = now + segment.timeout;
                RetransmissionResult::Continue
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                segment.retry_at = now + OUTBOUND_QUEUE_RETRY_DELAY;
                RetransmissionResult::Continue
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                retransmission.stopped = true;
                retransmission.segments.clear();
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
        retransmission.segments.clear();
        drop(retransmission);
        self.retransmission_notify.notify_one();
        self.wake_io();
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
}
