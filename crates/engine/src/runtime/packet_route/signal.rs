//! Runtime-neutral single-waiter receipt; each path has one owner and pump.
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    task::{Poll, Waker},
};
#[derive(Debug)]
pub(super) struct Signal {
    value: AtomicBool,
    waiter: Mutex<Option<Waker>>,
}
impl Signal {
    pub fn new(value: bool) -> Self {
        Self {
            value: AtomicBool::new(value),
            waiter: Mutex::new(None),
        }
    }
    pub fn get(&self) -> bool {
        self.value.load(Ordering::Acquire)
    }
    pub fn claim(&self) -> bool {
        if self
            .value
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let wake = self.waiter.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(wake) = wake {
            wake.wake();
        }
        true
    }
    pub fn set(&self, value: bool) {
        self.value.store(value, Ordering::Release);
        let wake = self.waiter.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    pub async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut waiter = self.waiter.lock().unwrap_or_else(|e| e.into_inner());
            if self.get() {
                Poll::Ready(())
            } else {
                *waiter = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}
