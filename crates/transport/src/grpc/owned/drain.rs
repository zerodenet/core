//! Confirm each longer physical consumer before its shorter original donor.
use super::{state::Physical, *};
use std::sync::Arc;
impl Managed {
    pub(super) async fn retire_originals(
        &self,
        mut records: Vec<Arc<Physical>>,
        end: Instant,
    ) -> Result<bool, ChannelError> {
        records.sort_by_key(|record| std::cmp::Reverse(record.binding.prefix.len()));
        for record in &records {
            record.stop.cancel();
            record.adoption.request_retirement();
        }
        for record in records {
            let settle = async {
                let auxiliary = self
                    .grants
                    .registration()
                    .observe()
                    .ok_or_else(|| error(self.instance, Stage::Close, Reason::Revoked))?
                    .slots()
                    .iter()
                    .filter_map(|slot| slot.auxiliary().cloned())
                    .find(|witness| record.adoption.matches_auxiliary(witness));
                if let Some(auxiliary) = auxiliary {
                    let joined = record.adoption.accept_auxiliary().await?;
                    if !joined
                        .as_ref()
                        .is_some_and(|joined| auxiliary.matches_joined(joined))
                    {
                        return Err(error(self.instance, Stage::Close, Reason::Internal));
                    }
                    let released = auxiliary.wait_registration_released().await;
                    if !auxiliary.matches_joined(&released) || !released.registration_released() {
                        return Err(error(self.instance, Stage::Close, Reason::Internal));
                    }
                }
                let driver = record.witness.lock().unwrap().clone();
                if let Some(driver) = driver {
                    let joined = driver.wait().await;
                    if !driver.matches_joined(&joined) {
                        return Err(error(self.instance, Stage::Close, Reason::Internal));
                    }
                    record.receive(joined);
                    let released = driver.wait_registration_released().await;
                    if !driver.matches_joined(&released) || !released.registration_released() {
                        return Err(error(self.instance, Stage::Close, Reason::Internal));
                    }
                }
                record.release_installed_pending();
                if !self.budget.wait_physical_returned(&record, end).await? {
                    return Ok(false);
                }
                let report = record.adoption.retire(end).await?;
                self.settled.lock().unwrap().append(report);
                if let Some(children) = &record.protocols {
                    let _returned = children.wait_returned(end).await?;
                    children.reclaim_returned();
                }
                let pending = record.pending.lock().unwrap().clone();
                for original in pending {
                    let report = original.retire(end).await?;
                    self.settled.lock().unwrap().append(report);
                }
                record.release_installed_pending();
                record.receive_available();
                Ok::<bool, ChannelError>(record.closed())
            };
            let closed = tokio::time::timeout_at(end, settle)
                .await
                .map_err(|source| {
                    error(self.instance, Stage::Close, Reason::Timeout).with_source(source)
                })??;
            if !closed {
                return Ok(false);
            }
        }
        self.harvest();
        Ok(self.records.lock().unwrap().is_empty())
    }
}
