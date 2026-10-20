//! Actual role handles with the selected host owner fence.
use super::*;

impl ComponentSlot {
    pub(crate) fn service_admission(&self) -> Option<super::super::ServiceAdmission> {
        Some(super::super::ServiceAdmission::new(
            self.admission.get()?.clone(),
            self.instance_id(),
            self.admission_owner.clone(),
            self.cancel.clone(),
        ))
    }
    pub(in crate::component) fn bind_admission(
        &self,
        host: Arc<crate::component::admission::HostAdmission>,
    ) {
        if let Err(host) = self.admission.set(host) {
            assert!(
                Arc::ptr_eq(self.admission.get().unwrap(), &host),
                "slot cannot move between hosts"
            );
        }
    }
    pub(in crate::component) fn admission_grant(&self) -> (ComponentInstanceId, Arc<()>) {
        (self.instance_id(), self.admission_owner.clone())
    }
    pub(crate) fn legacy_connect_stage(&self) -> &'static str {
        self.legacy_connect_stage
    }
    pub(crate) async fn stream(
        &self,
        deadline: Instant,
    ) -> Result<Arc<dyn DynStreamOutbound>, ChannelError> {
        self.start(deadline).await?;
        self.live
            .get()
            .and_then(|live| live.as_ref().ok())
            .and_then(|live| live.stream.clone())
            .ok_or_else(|| self.failure(Reason::Internal, "missing built Stream role"))
    }
    pub(crate) async fn datagram(
        &self,
        deadline: Instant,
    ) -> Result<Arc<dyn DynDatagramOutbound>, ChannelError> {
        self.start(deadline).await?;
        self.live
            .get()
            .and_then(|live| live.as_ref().ok())
            .and_then(|live| live.datagram.clone())
            .ok_or_else(|| self.failure(Reason::Internal, "missing built Datagram role"))
    }
}
