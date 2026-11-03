use super::*;
use tokio::time::timeout_at;
use zero_platform_tokio::contract::lifecycle::Unfinished;

impl Owner {
    async fn close_carriers(&mut self, deadline: Instant) -> Result<(), BuildError> {
        for carrier in &mut self.carriers {
            if let Some(owner) = &mut carrier.owner {
                // Persist each original result before awaiting another owner.
                self.pending_close.append(owner.shutdown(deadline).await?);
            }
        }
        Ok(())
    }
    pub(super) async fn close(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        self.pending_close.unfinished.clear();
        let Some(state) = self.state.clone() else {
            self.close_carriers(deadline).await?;
            return Ok(std::mem::take(&mut self.pending_close));
        };
        let (physical, drained) = tokio::join!(
            self.close_carriers(deadline),
            timeout_at(deadline, state.drained())
        );
        physical?;
        if drained.is_err() {
            self.pending_close.unfinished.push(Unfinished::Resource(
                "mieru pending opens, logical users or carrier scope joins".into(),
            ));
        }
        state.harvest_closed();
        if state.physical_unconfirmed() && self.pending_close.is_complete() {
            self.pending_close.unfinished.push(Unfinished::Io(
                "mieru original carrier physical closure is unconfirmed".into(),
            ));
        }
        if self.pending_close.is_complete() && self.declared {
            let confirmed = self.carriers.iter().all(|carrier| {
                let actual = carrier
                    .owner
                    .as_ref()
                    .expect("controlled declaration")
                    .registration();
                actual.closed().is_some_and(|proof| proof.matches(&actual))
                    && actual
                        .users_closed()
                        .is_some_and(|proof| proof.matches(&actual))
            });
            if confirmed {
                self.pool.confirm_retired(&state).map_err(|source| {
                    error(self.instance, Stage::Close, Reason::Io).with_source(source)
                })?;
            } else {
                self.pending_close.unfinished.push(Unfinished::Resource(
                    "mieru original carrier users closure".into(),
                ));
            }
        }
        Ok(std::mem::take(&mut self.pending_close))
    }
}
