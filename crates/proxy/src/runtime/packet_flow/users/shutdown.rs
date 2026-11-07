//! Same users owner drives retained view/closure/caller observations.
use super::*;
use tokio::time::timeout_at;
impl FlowUsers {
    /// Only logical view release and original caller joins. No native/socket,
    /// worker-scope ACK, recovery or replacement authorization is issued here.
    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.reap();
            let (count, missing_caller, unaccounted, first) = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                (
                    state.records.len(),
                    state.records.iter().any(|record| record.caller.is_none()),
                    state.unaccounted,
                    state.first.clone(),
                )
            };
            if let Some(first) = first {
                return Err(crate::component::instance::retained(first));
            }
            if unaccounted || missing_caller {
                let mut report = CloseReport::default();
                report.unfinished.push(Unfinished::Io(format!(
                    "packet endpoint users lack original caller provenance ({} retained views, unaccounted leaf: {unaccounted})", count
                )));
                return Ok(report);
            }
            if count == 0 {
                return Ok(CloseReport::default());
            }
            // Join observation neither consumes the ingress Scope receipt nor
            // ACKs its failure. That source remains owned by the actual host.
            let mut caller = None;
            for index in 0..count {
                let record = self
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .records
                    .get(index)
                    .cloned();
                let Some(record) = record else { break };
                if record.packet_closed.is_none() && record.packet_driver.is_none() {
                    if let Some(actual) =
                        record.caller.as_ref().filter(|actual| !actual.is_joined())
                    {
                        caller = Some(actual.clone());
                        break;
                    }
                }
            }
            timeout_at(deadline, async {
                match caller {
                    Some(caller) => {
                        tokio::select! { _ = caller.wait() => {}, _ = &mut changed => {} }
                    }
                    None => changed.await,
                }
            })
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        }
    }
}
