//! Two fixed registrations matching Tokio UDP's one reader / one writer.
//! Native pollers retain weak internal wakers, never the caller's RawWaker.
use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
#[derive(Clone, Copy)]
pub(super) enum Direction {
    Read = 0,
    Write = 1,
}
#[derive(Default)]
struct Caller {
    sequence: u64,
    waker: Option<Waker>,
}
pub(super) struct WakeRelay {
    callers: [Mutex<Caller>; 2],
    native: [Waker; 2],
    retired: AtomicBool,
    guarded: AtomicUsize,
    pending: AtomicU8,
    flushing: AtomicBool,
}
impl WakeRelay {
    pub(super) fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            callers: [Mutex::new(Caller::default()), Mutex::new(Caller::default())],
            native: std::array::from_fn(|index| {
                Waker::from(Arc::new(NativeWake {
                    relay: weak.clone(),
                    bit: 1 << index,
                }))
            }),
            retired: AtomicBool::new(false),
            guarded: AtomicUsize::new(0),
            pending: AtomicU8::new(0),
            flushing: AtomicBool::new(false),
        })
    }
    #[cfg(test)]
    pub(super) fn assert_unlocked(&self) {
        for slot in &self.callers {
            drop(
                slot.try_lock()
                    .expect("caller callback ran under relay gate"),
            );
        }
        assert_eq!(
            self.guarded.load(Ordering::Acquire),
            0,
            "caller callback ran during native poll deferral"
        );
    }
    pub(super) fn mark_retired(&self) {
        self.retired.store(true, Ordering::Release);
    }
    pub(super) fn notify_all(&self) {
        self.signal(3);
    }
    pub(super) fn prepare(
        self: &Arc<Self>,
        direction: Direction,
        cx: &Context<'_>,
    ) -> std_io::Result<PreparedPoll> {
        if self.retired.load(Ordering::Acquire) {
            return Err(retired());
        }
        // Arbitrary caller clone/drop code runs before any native/retired gate.
        let mut new = Some(cx.waker().clone());
        let (old, sequence) = {
            let mut caller = self.callers[direction as usize]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.retired.load(Ordering::Acquire) {
                (None, caller.sequence)
            } else {
                let Some(sequence) = caller.sequence.checked_add(1) else {
                    drop(caller);
                    drop(new);
                    return Err(std_io::Error::other(
                        "UDP caller registration identity exhausted",
                    ));
                };
                caller.sequence = sequence;
                (std::mem::replace(&mut caller.waker, new.take()), sequence)
            }
        };
        drop(old);
        drop(new);
        if self.retired.load(Ordering::Acquire) {
            return Err(retired());
        }
        let mut count = self.guarded.load(Ordering::Acquire);
        loop {
            let next = count
                .checked_add(1)
                .ok_or_else(|| std_io::Error::other("UDP poll deferral counter exhausted"))?;
            match self.guarded.compare_exchange_weak(
                count,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => count = current,
            }
        }
        Ok(PreparedPoll {
            relay: self.clone(),
            direction,
            sequence,
            pending: false,
        })
    }
    fn signal(&self, bit: u8) {
        self.pending.fetch_or(bit, Ordering::AcqRel);
        self.flush();
    }
    fn flush(&self) {
        loop {
            if self.guarded.load(Ordering::Acquire) != 0
                || self
                    .flushing
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return;
            }
            let flushing = Flushing(&self.flushing);
            while self.guarded.load(Ordering::Acquire) == 0 {
                let pending = self.pending.swap(0, Ordering::AcqRel);
                if pending == 0 {
                    break;
                }
                let wake: [Option<Waker>; 2] = std::array::from_fn(|index| {
                    if pending & (1 << index) == 0 {
                        return None;
                    }
                    self.callers[index]
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .waker
                        .take()
                });
                // Taking both slots first permits synchronous close/re-poll,
                // without recursion, user code under a mutex or a wake queue.
                for caller in wake.into_iter().flatten() {
                    caller.wake();
                }
            }
            drop(flushing);
            if self.guarded.load(Ordering::Acquire) != 0
                || self.pending.load(Ordering::Acquire) == 0
            {
                return;
            }
        }
    }
}
fn retired() -> std_io::Error {
    std_io::Error::new(
        std_io::ErrorKind::NotConnected,
        "original UDP poll is retired",
    )
}
struct NativeWake {
    relay: Weak<WakeRelay>,
    bit: u8,
}
impl Wake for NativeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(relay) = self.relay.upgrade() {
            relay.signal(self.bit);
        }
    }
}
struct Flushing<'a>(&'a AtomicBool);
impl Drop for Flushing<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
/// Declare before all native/retired guards. Drop after those guards and after
/// actual observation; this is wake deferral, never physical-close evidence.
pub(super) struct PreparedPoll {
    relay: Arc<WakeRelay>,
    direction: Direction,
    sequence: u64,
    pending: bool,
}
impl PreparedPoll {
    pub(super) fn pending(&mut self, pending: bool) {
        self.pending = pending;
    }
    pub(super) fn context(&self) -> Context<'_> {
        Context::from_waker(&self.relay.native[self.direction as usize])
    }
}
impl Drop for PreparedPoll {
    fn drop(&mut self) {
        let last = self.relay.guarded.fetch_sub(1, Ordering::AcqRel) == 1;
        if std::thread::panicking() {
            return;
        }
        if !self.pending {
            let old = {
                let mut caller = self.relay.callers[self.direction as usize]
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if caller.sequence == self.sequence {
                    caller.waker.take()
                } else {
                    None
                }
            };
            // A completed operation retains no caller task. The exact checked
            // registration ID prevents clearing another simultaneous poll.
            drop(old);
        }
        if last {
            self.relay.flush();
        }
    }
}
