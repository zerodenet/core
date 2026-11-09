//! Per-half native/users closure and the one real caller Scope remain separate.
use super::*;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    CloseReason,
};
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        if self.started {
            return self.session.start(end).await;
        }
        if !self.registered[0] {
            self.upload.register()?;
            self.registered[0] = true;
        }
        if !self.registered[1] {
            if let Some(download) = &self.download {
                download.register()?;
            }
            self.registered[1] = true;
        }
        self.upload.start_material(end).await?;
        if let Some(download) = &self.download {
            let shared_material = self
                .upload
                .material()
                .zip(download.material())
                .is_some_and(|(up, down)| Arc::ptr_eq(up, down));
            if !shared_material {
                download.start_material(end).await?;
            }
        }
        self.upload.start_originals(end).await?;
        if let Some(download) = &mut self.download {
            download.start_originals(end).await?;
        }
        self.session.start(end).await?;
        self.started = true;
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
        self.upload.revoke();
        if let Some(download) = &self.download {
            download.revoke();
        }
        entry::Table::request_retirement(&self.shared.entries);
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let wait = async {
            let mut ordinal = 0;
            while let Some((next, entry)) = entry::Table::next_entry(&self.shared.entries, ordinal)
            {
                ordinal = next;
                entry.retirement.wait_protocol_original().await?;
            }
            Ok::<_, ChannelError>(())
        };
        match tokio::time::timeout_at(end, wait).await {
            Ok(result) => result?,
            Err(_) => {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "actual mixed XHTTP cleanup remains Root-held".into(),
                    )],
                    ..Default::default()
                })
            }
        }
        let (up, down) = tokio::join!(self.upload.shutdown_originals(end), async {
            match self.download.as_mut() {
                Some(down) => down.shutdown_originals(end).await,
                None => Ok(CloseReport::default()),
            }
        });
        let mut report = CloseReport::default();
        // Both branches have advanced before either typed result is observed.
        report.append(up?);
        report.append(down?);
        entry::Table::reap(&self.shared.entries);
        report.append(self.session.shutdown(end).await?);
        Ok(report)
    }
}
