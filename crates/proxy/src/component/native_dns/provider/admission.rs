//! One network/resource fence shared by plain and ancillary native queries.
use super::*;
impl Provider {
    pub(super) async fn admit(
        &self,
        caller: &TaskJoinWitness,
        cancel: &CancellationToken,
        end: tokio::time::Instant,
        cost: usize,
    ) -> std::io::Result<tokio::sync::MutexGuard<'_, u64>> {
        let checked = || {
            if tokio::time::Instant::now() >= end {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "native DNS operation deadline elapsed",
                ));
            }
            if self.cancel.is_cancelled()
                || cancel.is_cancelled()
                || caller.is_joined()
                || !caller
                    .scope_identity()
                    .same_scope(&self.registrar.scope_identity())
            {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "DNS query is not owned by this active original scope",
                ))
            } else {
                Ok(())
            }
        };
        checked()?;
        let mut epoch = tokio::time::timeout_at(end, self.epoch.lock())
            .await
            .map_err(std::io::Error::other)?;
        checked()?;
        let current = self.egress.generation();
        self.tasks.fence_network(current, caller, end).await?;
        if *epoch != current {
            let old = self
                .users
                .lock()
                .expect("DNS users poisoned")
                .iter()
                .map(|entry| (entry.task.clone(), entry.cancel.clone()))
                .collect::<Vec<_>>();
            #[cfg(feature = "dns")]
            let old = {
                let mut old = old;
                old.extend(
                    self.quic_users
                        .lock()
                        .expect("DNS QUIC users poisoned")
                        .iter()
                        .map(|entry| (entry.task.clone(), entry.cancel.clone())),
                );
                old
            };
            for (_, cancel) in &old {
                cancel.cancel();
            }
            if old.iter().any(|(task, _)| task.same_registration(caller)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "old DNS query cannot migrate itself into a new network",
                ));
            }
            for (task, _) in old {
                tokio::time::timeout_at(end, task.wait())
                    .await
                    .map_err(std::io::Error::other)?;
            }
            self.reap(end).await?;
            if self.retained() != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "old DNS original native close remains unconfirmed",
                ));
            }
            *epoch = current;
        }
        self.reap(end).await?;
        checked()?;
        if self.retained().saturating_add(cost) > LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "native DNS original socket capacity exhausted",
            ));
        }
        Ok(epoch)
    }
}
