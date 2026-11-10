//! Exact live provider value; no Weak expiry becomes an original-close fact.
use super::*;
use zero_platform_tokio::contract::CancellationToken;
pub struct Active {
    pub(in crate::transport) instance: ComponentInstanceId,
    pub(in crate::transport) scope: TaskScopeIdentity,
    dialer: BrowserDialer,
    pub(super) cancel: CancellationToken,
    pub(super) ready: AtomicBool,
}
impl Active {
    pub(super) fn new(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        dialer: BrowserDialer,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            instance,
            scope,
            dialer,
            cancel,
            ready: AtomicBool::new(false),
        }
    }
    pub(in crate::transport) fn dialer(&self) -> Result<BrowserDialer, ChannelError> {
        if !self.ready.load(Ordering::Acquire)
            || self.cancel.is_cancelled()
            || !self.scope.resource_open()
        {
            return Err(error(Reason::Revoked));
        }
        Ok(self.dialer.clone())
    }
}
