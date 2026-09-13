use super::*;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::time::Instant;
pub(super) struct Group {
    pub(super) connections: connection::Connections,
    pub(super) factory: XhttpCarrierFactory,
    pub(super) keepalive: i64,
    usage: AtomicUsize,
    remaining_uses: AtomicI64,
    remaining_requests: AtomicI64,
    expires: Option<Instant>,
    closed: AtomicBool,
    idle_since: Mutex<Instant>,
}
impl Group {
    pub(super) fn new(config: SplitHttpXmux, factory: XhttpCarrierFactory) -> Self {
        let uses = super::super::request::sample(config.c_max_reuse_times);
        let requests = super::super::request::sample(config.h_max_request_times);
        let seconds = super::super::request::sample(config.h_max_reusable_secs);
        Self {
            connections: connection::Connections::default(),
            factory,
            keepalive: config.h_keep_alive_period,
            usage: AtomicUsize::new(0),
            remaining_uses: AtomicI64::new(if uses == 0 { -1 } else { uses as i64 }),
            remaining_requests: AtomicI64::new(if requests == 0 {
                i64::MAX
            } else {
                requests as i64
            }),
            expires: (seconds > 0)
                .then(|| Instant::now() + std::time::Duration::from_secs(seconds as u64)),
            closed: AtomicBool::new(false),
            idle_since: Mutex::new(Instant::now()),
        }
    }
    pub(super) fn active(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }
    pub(super) fn reusable(&self) -> bool {
        self.remaining_uses.load(Ordering::Acquire) != 0
            && self.request_reusable()
            && !(self.active() == 0
                && self.idle_since.lock().unwrap().elapsed() >= std::time::Duration::from_secs(300))
    }
    pub(super) fn request_reusable(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && self.remaining_requests.load(Ordering::Acquire) > 0
            && self.expires.is_none_or(|time| Instant::now() < time)
    }
    pub(super) fn take_request(&self) -> bool {
        self.request_reusable() && self.remaining_requests.fetch_sub(1, Ordering::AcqRel) > 0
    }
    pub(super) fn fail(&self) {
        self.closed.store(true, Ordering::Release);
    }
}
// One lease per logical stream. The upload may replace it when request/age
// limits expire; the old download retains its connection until its own EOF.
pub(in crate::split_http) struct Usage {
    pub(super) group: Arc<Group>,
}
impl Usage {
    pub(super) fn new(group: Arc<Group>) -> Self {
        group.usage.fetch_add(1, Ordering::AcqRel);
        if group.remaining_uses.load(Ordering::Acquire) > 0 {
            group.remaining_uses.fetch_sub(1, Ordering::AcqRel);
        }
        Self { group }
    }
}
impl Drop for Usage {
    fn drop(&mut self) {
        if self.group.usage.fetch_sub(1, Ordering::AcqRel) == 1 {
            *self.group.idle_since.lock().unwrap() = Instant::now();
        }
    }
}
