//! A read-only CA snapshot is prepared after its real resource owner is installed.
use super::*;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    lifecycle::{CloseReport, ResourceLifecycle},
    CloseReason,
};

type Prepared = Arc<Mutex<Option<zero_transport::tls::PreparedClient>>>;
pub(super) struct CaOwner {
    owner: SessionOwner,
    admission: Arc<SessionAdmission>,
    options: Arc<Options>,
    prepared: Prepared,
    resources: Arc<dyn ResourceAccess>,
}
impl CaOwner {
    pub(super) fn new(
        owner: SessionOwner,
        admission: Arc<SessionAdmission>,
        options: Arc<Options>,
        prepared: Prepared,
        resources: Arc<dyn ResourceAccess>,
    ) -> Self {
        Self {
            owner,
            admission,
            options,
            prepared,
            resources,
        }
    }
}
impl ResourceLifecycle for CaOwner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if Instant::now() >= deadline {
            return Err(self.admission.error(
                None,
                Stage::Prepare,
                Reason::Timeout,
                DeliveryState::NotSubmitted,
            ));
        }
        let ready = self
            .prepared
            .lock()
            .expect("prepared VMess TLS state")
            .is_some();
        if !ready {
            let file = self.options.ca_file.as_ref().expect("declared CA file");
            let reading = self.resources.read_file(file.path.clone(), file.max_bytes);
            let bytes = tokio::select! { biased;
                _ = self.admission.cancelled() => return Err(self.admission.error(None,Stage::Prepare,Reason::Revoked,DeliveryState::NotSubmitted)),
                result = with_deadline(Some(deadline), reading) => result.map_err(|source| self.admission.error(None,Stage::Prepare,Reason::Timeout,DeliveryState::NotSubmitted).with_source(source))?,
            }.map_err(|error| self.admission.attach(error, None))?;
            if bytes.len() > file.max_bytes {
                return Err(self
                    .admission
                    .error(
                        None,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "CA File grant returned more than its declared bound",
                    )));
            }
            let prepared = zero_transport::tls::PreparedClient::from_ca_pem(
                self.options.tls.as_ref().expect("CA TLS profile"),
                file.source_dir.as_deref(),
                &bytes,
            )
            .map_err(|source| {
                self.admission
                    .error(
                        None,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source)
            })?;
            *self.prepared.lock().expect("prepared VMess TLS state") = Some(prepared);
        }
        // Admission becomes ready only after the authorized snapshot is usable.
        self.owner.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.owner.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        let report = self.owner.shutdown(deadline).await?;
        self.prepared
            .lock()
            .expect("prepared VMess TLS state")
            .take();
        // ResourceAccess returned bytes, not a live file handle. This reports
        // admission retirement, never the closure of transferred TLS streams.
        Ok(report)
    }
}
