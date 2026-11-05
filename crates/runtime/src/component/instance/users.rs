//! Transferred role users are distinct from native I/O and task close evidence.
//! Counters belong to actual resource generations, including historical roles.
use super::*;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
mod datagram;
mod packet;
mod stream;

pub(in crate::component) struct Users {
    component: ComponentId,
    enabled: OnceLock<bool>,
    active: AtomicUsize,
    cancel: CancellationToken,
    changed: Notify,
}
pub(in crate::component) struct Lease {
    owners: Vec<Arc<Users>>,
    component: ComponentId,
    session: Option<zero_core::component::SessionId>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        for owner in &self.owners {
            if owner.active.fetch_sub(1, Ordering::AcqRel) == 1 {
                owner.changed.notify_waiters();
            }
        }
    }
}
impl Users {
    pub(in crate::component) fn new(component: ComponentId) -> Self {
        Self {
            component,
            enabled: OnceLock::new(),
            active: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            changed: Notify::new(),
        }
    }
    pub(in crate::component) fn bind(&self, required: bool) {
        if let Err(required) = self.enabled.set(required) {
            assert_eq!(
                self.enabled.get(),
                Some(&required),
                "one user policy per generation"
            );
        }
    }
    pub(in crate::component) fn revoke(&self) {
        if self.enabled.get().copied().unwrap_or(false) {
            self.cancel.cancel();
        }
    }
    fn acquire(&self) -> Result<(), ChannelError> {
        let mut count = self.active.load(Ordering::Acquire);
        loop {
            let next = count
                .checked_add(1)
                .ok_or_else(|| invalid(Reason::Capacity, "component user count exhausted"))?;
            match self.active.compare_exchange_weak(
                count,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => count = current,
            }
        }
    }
    #[cfg(test)]
    fn enter(
        self: &Arc<Self>,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<Option<Arc<Lease>>, ChannelError> {
        Lease::enter(self.component, std::slice::from_ref(self), session)
    }
    pub(in crate::component) async fn wait_idle(&self, deadline: Instant) -> CloseReport {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return CloseReport::default();
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return CloseReport {
                    unfinished: vec![
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(format!(
                            "component {} has {} transferred users",
                            self.component.0,
                            self.active.load(Ordering::Acquire)
                        )),
                    ],
                    ..Default::default()
                };
            }
        }
    }
}
impl Lease {
    pub(in crate::component) fn enter(
        component: ComponentId,
        roots: &[Arc<Users>],
        session: Option<zero_core::component::SessionId>,
    ) -> Result<Option<Arc<Self>>, ChannelError> {
        if roots.is_empty() {
            return Ok(None);
        }
        let mut lease = Self {
            owners: Vec::with_capacity(roots.len()),
            component,
            session,
        };
        for root in roots {
            root.acquire()?;
            // Retain every acquired count before the revocation check. Any
            // subsequent error drops this exact partial lease, not a guessed count.
            lease.owners.push(root.clone());
            if root.cancel.is_cancelled() {
                let mut error = lease.failure(Stage::Dial);
                error.delivery = DeliveryState::NotSubmitted;
                return Err(error);
            }
        }
        Ok(Some(Arc::new(lease)))
    }
    fn is_revoked(&self) -> bool {
        self.owners.iter().any(|owner| owner.cancel.is_cancelled())
    }
    pub(in crate::component) async fn revoked(&self) {
        self.cancellation().await
    }
    fn cancellation(&self) -> zero_platform_tokio::contract::BoxFuture<'static, ()> {
        let mut waits: Vec<_> = self
            .owners
            .iter()
            .map(|owner| Box::pin(owner.cancel.clone().cancelled_owned()))
            .collect();
        Box::pin(std::future::poll_fn(move |cx| {
            for wait in &mut waits {
                if wait.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
            }
            std::task::Poll::Pending
        }))
    }
    pub(in crate::component) fn failure(&self, stage: Stage) -> ChannelError {
        let mut error = invalid(Reason::Revoked, "component resource user was revoked");
        error.stage = stage;
        error.component = Some(self.component);
        error.session = self.session;
        error.delivery = DeliveryState::Unknown;
        error.impact = Impact::Session;
        error
    }
    pub(in crate::component) fn stream(
        self: Arc<Self>,
        io: zero_platform_tokio::contract::stream::BoxStream,
    ) -> zero_platform_tokio::contract::stream::BoxStream {
        Box::pin(stream::Transferred {
            revoked: self.cancellation(),
            io,
            _lease: self,
        })
    }
    pub(in crate::component) fn packet(
        self: Arc<Self>,
        session: zero_platform_tokio::contract::packet::BoxPacketSession,
    ) -> zero_platform_tokio::contract::packet::BoxPacketSession {
        packet::wrap(self, session)
    }
    pub(in crate::component) fn datagram(
        self: Arc<Self>,
        session: zero_platform_tokio::contract::datagram::BoxDatagramSession,
    ) -> zero_platform_tokio::contract::datagram::BoxDatagramSession {
        datagram::wrap(self, session)
    }
    pub(in crate::component) fn datagram_loan(
        self: Arc<Self>,
        loan: &mut zero_platform_tokio::controlled_carrier::loan::DatagramLoan,
    ) {
        datagram::wrap_loan(self, loan);
    }
}
#[cfg(test)]
#[path = "../tests/transferred_users.rs"]
mod tests;
