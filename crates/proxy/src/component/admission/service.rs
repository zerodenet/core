//! Observable admission for a real retained Service carrier dependency.
use super::*;
use zero_platform_tokio::contract::CancellationToken;

pub(crate) struct ServiceAdmission {
    host: Arc<HostAdmission>,
    instance: ComponentInstanceId,
    owner: Arc<()>,
    cancel: CancellationToken,
}
impl std::fmt::Debug for ServiceAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAdmission")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}
impl ServiceAdmission {
    pub(in crate::component) fn new(
        host: Arc<HostAdmission>,
        instance: ComponentInstanceId,
        owner: Arc<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            host,
            instance,
            owner,
            cancel,
        }
    }
    pub(crate) fn check_current(&self) -> Result<(), ChannelError> {
        let view = self.host.0.read().expect("host admission poisoned");
        if !self.cancel.is_cancelled()
            && view
                .current
                .get(&self.instance)
                .into_iter()
                .chain(view.preparing.get(&self.instance))
                .any(|token| Arc::ptr_eq(token, &self.owner))
        {
            return Ok(());
        }
        let mut error = invalid(
            Reason::StalePlan,
            "Service carrier component is no longer admitted",
        );
        error.component = Some(self.instance.component);
        Err(error)
    }
    pub(crate) async fn revoked(&self) -> ChannelError {
        loop {
            let changed = self.host.1.notified();
            if let Err(error) = self.check_current() {
                return error;
            }
            tokio::select! {
                _ = changed => {},
                _ = self.cancel.cancelled() => {},
            }
        }
    }
}
