use super::*;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::time::Instant;
pub(super) struct Group {
    pub(super) connections: connection::Connections,
    pub(super) keepalive: i64,
    owner: Option<OwnerKey>,
    usage: AtomicUsize,
    responses: AtomicUsize,
    remaining_uses: AtomicI64,
    remaining_requests: AtomicI64,
    expires: Option<Instant>,
    closed: AtomicBool,
    idle_since: Mutex<Instant>,
}
impl Group {
    pub(super) fn new(config: SplitHttpXmux, owner: Option<OwnerKey>) -> Self {
        let uses = super::super::request::sample(config.c_max_reuse_times);
        let requests = super::super::request::sample(config.h_max_request_times);
        let seconds = super::super::request::sample(config.h_max_reusable_secs);
        Self {
            connections: connection::Connections::default(),
            keepalive: config.h_keep_alive_period,
            owner,
            usage: AtomicUsize::new(0),
            responses: AtomicUsize::new(0),
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
    pub(super) fn same_factory(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.instance == instance && owner.scope.same_scope(scope))
    }
    pub(super) fn matches_owner(&self, owner: Option<&OwnerKey>) -> bool {
        match (&self.owner, owner) {
            (None, None) => true,
            (Some(actual), Some(owner)) => actual.same(owner),
            _ => false,
        }
    }
    pub(super) fn has_responses(&self) -> bool {
        self.responses.load(Ordering::Acquire) != 0
    }
    pub(super) fn active(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }
    pub(super) fn reusable(&self) -> bool {
        self.remaining_uses.load(Ordering::Acquire) != 0
            && self.request_reusable()
            && !(self.active() == 0
                && self.responses.load(Ordering::Acquire) == 0
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
    pub(super) group: GroupRef,
}
impl Usage {
    pub(super) fn new(group: GroupRef) -> Self {
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

// A real HTTP body pins its own original even after request/age rotation moves
// a logical Usage to another group. This is not a second logical concurrency.
pub(super) struct ResponseUse(GroupRef);
impl ResponseUse {
    pub fn new(group: GroupRef) -> Self {
        group.responses.fetch_add(1, Ordering::AcqRel);
        Self(group)
    }
}
impl Drop for ResponseUse {
    fn drop(&mut self) {
        if self.0.responses.fetch_sub(1, Ordering::AcqRel) == 1 && self.0.active() == 0 {
            *self.0.idle_since.lock().unwrap() = Instant::now();
        }
    }
}
